require 'csv'

class MySqliteRequest
  def initialize
    @request = []
    @selecetedColumns = []
    @byCriteria = []
    @sortedTable = []
    @tableName = []
    @csvName = []
    @tableHeads = []
    @checkMethod = ''
    self
  end

  def update_file(file_name, col_name, val)
    rows = CSV.read(file_name, headers: true)
    key = @newData.keys
    val = @newData.values
    len = key.length
    for i in 0..len-1
      rows.each do |row|
        row[key[i]] = val[i]
      end
    end
    CSV.open(file_name, 'w', headers: true) do |csv|
      csv << rows.headers
      rows.each { |row| csv << row }
    end
  end

  def updated_file(file_name, col_name, val)
    rows = CSV.read(file_name, headers: true)
    name = ''
    key = ''
    @newData.each do |_key, val|
      key = _key
      name = val
    end
    rows.each do |row|
      if row[col_name] == val
        # Update the desired columns
        row[key] = name
      end
    end
    # Write the modified rows back to the file
    CSV.open(file_name, 'w', headers: true) do |csv|
      csv << rows.headers
      rows.each { |row| csv << row }
    end
  end

  def delete_from_file(file_name, col_name, val)
    rows = CSV.read(file_name, headers: true)
    rows.delete_if { |row| row[col_name] == val }
    CSV.open(file_name, 'w', headers: true) do |csv|
      csv << rows.headers
      rows.each { |row| csv << row }
    end
  end

  def joinTables(table_a, table_b, column_on_db_a, column_on_db_b)
    tempTableA = []
    tempTableB = []
    joinedTable = []

    table_a.each do |hash|
      table_b.each do |h|
        if hash[column_on_db_a] == h[column_on_db_b]
          tempTableA.push(hash)
          tempTableB.push(h)
        end
      end
    end
    tempTableA.each do |hash|
      newHash = Hash.new(0)
      tempTableB.each do |h|
        newHash = hash.merge!(h.select { |k, _| !hash.has_key? k })
      end
      joinedTable.push(newHash)
    end
    joinedTable
  end

  def update_op(new_data)
    @newData = new_data
  end

  def insert_op(list_of_hashes, new_hash)
    if new_hash.class == Array
      new_hash.each do |data|
        list_of_hashes << data
      end
    else
      list_of_hashes << new_hash
    end
    @request = list_of_hashes
  end

  def write_to_file(list_of_hashes, db_name)
    CSV.open(db_name, 'w', headers: true) do |csv|
      return if list_of_hashes.length == 0

      csv << list_of_hashes[0].keys # how to fix this???
      list_of_hashes.each do |hash|
        csv << CSV::Row.new(hash.keys, hash.values)
      end
    end
  end

  def readFromCSVFile(table_name)
    table = nil
    filename_db = table_name
    if filename_db
      table = CSV.foreach(filename_db, headers: true).map { |row| row.to_h }
    else
      print 'No such file'
      return nil
    end
    table
  end

  def csv_to_hash(csv_file)
    @tableName = csv_file
    table = readFromCSVFile(csv_file)
    table
  end

  def where_select(column_name, criteria)
    self if !column_name || !criteria
    if @column_name.class == Array
      # puts @column_name
      whereArr = []
      @table.map do |hash|
        @column_name.each do |n|
          whereArr << hash[n] if hash[column_name] == criteria
        end
      end

      result = whereArr.each_slice(2).map do |a, b|
        { @column_name[0] => a, @column_name[1] => b }
      end
      # puts "result = #{result}"
      @request = result
      # puts "request = #{@request}"

    else
      @table.map do |hash|
        @byCriteria << hash[@column_name] if hash[column_name] == criteria
      end
      newArr = []
      @byCriteria.each do |val|
        ha = { @column_name => val }
        newArr << ha
      end
      @request = newArr
    end
  end

  def where_update(column_name, criteria)
    self if !column_name || !criteria
    key = @newData.keys
    val = @newData.values
    len = key.length
    elem = @request.find_all {|hash| hash[column_name] == criteria}
    for i in 0..len-1
      elem.each do |hash|
        hash[key[i]] = val[i]
      end
    end
    update_file(@file_name, column_name, criteria)
  end

  def where_updated(column_name, criteria)
    self if !column_name || !criteria
    name = ''
    key = ''
    @newData.each do |_key, val|
      key = _key
      name = val
    end
    # puts name.class
    @request.each do |hash|
      if hash[column_name] == criteria
        hash[key] = name
        # change the val
      end
    end
    update_file(@file_name, column_name, criteria)
  end

  def where_delete(column_name, criteria)
    index = 0
    @request.each do |hash|
      # puts "hash = #{hash}"
      index = @request.index(hash) if hash[column_name] == criteria
    end
    @request.delete_at(index)
    delete_from_file(@csvName, column_name, criteria)
  end

  def from(table_name)
    @table = csv_to_hash(table_name)
    @csvName = table_name
    @tableHeads << @table[0].keys
    # puts @tableHeads
    @request = @table
    @request
    self
  end

  def select(column_name)
    @column_name = column_name
    @checkMethod = 'select'
    if column_name == '*'
      @selecetedColumns = @table
    else
      @table.map do |hash|
        newHash = nil
        newHash = if column_name.class == Array
                    hash.select { |key, _| column_name.include? key }
                  else
                    hash.select { |key, _| key == column_name }
                  end
        @selecetedColumns << newHash
      end
    end
    @request = @selecetedColumns
    # puts @request
    self
  end

  def where(column_name, criteria)
    # select" update, delete
    if @checkMethod == 'select'
      where_select(column_name, criteria)
    elsif @checkMethod == 'update'
      where_update(column_name, criteria)
    elsif @checkMethod == 'delete'
      where_delete(column_name, criteria)
    end
    self
  end

  def insert(file_name)
    @checkMethod = 'insert'
    @file_name = file_name
    table_hash = csv_to_hash(file_name)
    @request = table_hash
    self
  end

  def inserting(table_name)
    parsed_csv = insert_op(@request, @data) unless @data.nil?
    write_to_file(@request, table_name)
  end

  def inserted(table_name)
    newTable = csv_to_hash(table_name)
    headers = []
    newTable[0].each do |key, _val|
      headers << key
    end
    CSV.open(@csvName, 'a+') do |csv|
      csv << headers
      @data.each do |hash|
        csv << hash.values
      end
    end
    self
  end

  def values(data)
    @data = data
    if @checkMethod == 'insert'
      insert_op(@request, @data)
      write_to_file(@request, @file_name)
    elsif @checkMethod == 'update'
      update_op(@data)
    end
    # @checkMethod = ''
    self
  end

  def update(file_name)
    @file_name = file_name
    @checkMethod = 'update'
    table_hash = csv_to_hash(file_name)
    @request = table_hash
    self
  end

  def delete
    @checkMethod = 'delete'
    self
  end

  def join(column_on_db_a, filename_db_b, column_on_db_b)
    puts column_on_db_a
    @joined_table = []
    @second_table = readFromCSVFile(filename_db_b)
    @joined_table = joinTables(@table, @second_table, column_on_db_a, column_on_db_b)
    @request = @joined_table
    self
  end

  def order(order, column_name)
    @sortedTable = @request.sort_by { |k| k[column_name] }
    @sortedTable.reverse! if order == 'DESC'
    @request = @sortedTable
    self
  end

  def run
    puts @request
  end
end
# name, year_start, year_end, positon, height, weight, birth_date, college
# request = MySqliteRequest.new
# request = request.delete
# request = request.from('nba_player_data.csv')
# request = request.where('name', 'Matt Zunic')
# request = request.update("nba_player_data.csv")
# request = request.values({"name": "Akinade"})
# request = request.where("name", "Ivica Zubac")
# request = request.from('nba_player_data.csv')
# request = request.select(["name", "birth_date"])
# request = request.where("college", "University of California")
# request = request.where("year_start", "1997")
# request = request.insert("nba_player_data.csv")
# request = request.values({'name' => 'Alaa Abdelnaby', 'year_start' => '1991', 'year_end' => '1995', 'position' => 'F-C', 'height' => '6-10', 'weight' => '240', 'birth_date' => "June 24, 1968", 'college' => 'Duke University'})
# request = request.values([{"name" => "Ogunrinde Timi", "school" => "Comm Sec Sch, Olodo", "age" => "14", "class" => "JS3"}, {"name" => "Ogunrinde Timi2", "school" => "Comm Sec Sch, Olodo2", "age" => "18", "class" => "SS3"}])
# request = request.values({"name" => "Ogunrinde Timi", "school" => "Comm Sec Sch, Olodo", "age" => "14", "class" => "JS3"})
# request = request.insert('nba_player_data.csv')
# request = request.join("Player", "nba_player_data.csv", "name")
# request.run
# request.runputs "Hello, World!"

# Part I - Does it work to select name from nba player data?

# request = MySqliteRequest.new
# request = request.from('nba_player_data.csv')
# request = request.select('name')
# request = request.select(["name", "height"])
# request.run

# Part I - Does it work to select name from nba player data with a where?

# request = MySqliteRequest.new
# request = request.from('nba_player_data.csv')
# request = request.select(%w[name birth_date])
# request = request.where('college', 'University of California')
# request = request.order('DESC', 'name')
# request.run

# Part I - Does it work to select name from nba player data with multiple where?

# request = MySqliteRequest.new
# request = request.from('nba_player_data.csv')
# request = request.select(["name", "height"])
# request = request.select('name')
# request = request.where('college', 'University of California')
# request = request.join("year_start", "nba_players.csv", "Player")
# request = request.where('year_start', '1997')
# request.run

# Part I - Does it work to insert a nba player?

# request = MySqliteRequest.new
# request = request.insert('nba_player_data.csv')
# request = request.values({'name' => 'Alaa Abdelnaby', 'year_start' => '1991', 'year_end' => '1995', 'position' => 'F-C', 'height' => '6-10', 'weight' => '240', 'birth_date' => "June 24, 1968", 'college' => 'Duke University'})
# request.run

# Part I - Does it work to update a nba player?

request = MySqliteRequest.new
request = request.update('students.csv')
request = request.values({'name' => 'Mayowa', 'blog' => 'my blog', 'lastname' => 'Bello'})
request = request.where('email', 'mila@janedoe.com')
request.run

# Part I - Does it work to delete a nba player?

# request = MySqliteRequest.new
# request = request.delete()
# request = request.from('students.csv')
# request = request.where('born', '1921')
# request.run
