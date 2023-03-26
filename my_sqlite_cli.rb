require "readline"
require "./my_sqlite_request.rb"

#Test cases

#SELECT * FROM students
#SELECT name,email FROM students WHERE name = 'Mila'
#SELECT email FROM students

#INSERT INTO students VALUES (John,john@johndoe.com,A,https://blog.johndoe.com)

#UPDATE students SET email = 'jane@janedoe.com', blog = 'https://blog.janedoe.com' WHERE name = 'Mila'

#DELETE FROM students WHERE name = 'John'

#SELECT name,email FROM students ORDER BY name ASC
#SELECT * FROM students ORDER BY name DESC

#SELECT * FROM students JOIN details ON students.name = details.name

def bufferSplit(buffer, val)
    return buffer.split(val)
end

def removeQuotes(val)
    value = val.chop
    value[0] = ""
    return value
end

def readForWhere(array)

    hash = Hash.new(0)
    i = array.index("WHERE") + 1
    conditionField = array[i]
    conditionValue = array[i + 2]
    conditionValue = removeQuotes(conditionValue)

    hash[conditionField] = conditionValue

    return hash
end

def readForSelect(buffer) #SELECT

    arrayRequest = bufferSplit(buffer, " ")
    columnsName = nil
    tableName = nil
    joinedTable = nil
    conditionField = nil
    conditionValue = nil
    request = nil

    i = 1;

    columnsName = arrayRequest[i]  #"email,name"

    if columnsName.include? ","
        columnsName = columnsName.split(",")
    end

    #skip FROM word in command line
    i = arrayRequest.index("FROM") + 1

    #continue to read the request for table name
    tableName = arrayRequest[i]
    i = i + 1

    #continue to read for conditions if that exists
    if arrayRequest.include? "WHERE"

        conditions = readForWhere(arrayRequest)
        conditionField = conditions.keys[0]
        conditionValue = conditions.values[0]
        request = MySqliteRequest.new().from(tableName).select(columnsName).where(conditionField, conditionValue).run()

    elsif arrayRequest.include? "ORDER" 

        i = arrayRequest.index("BY") + 1
        columnName = arrayRequest[i]
        order = arrayRequest[i + 1]
        request = MySqliteRequest.new().from(tableName).select(columnsName).order(order, columnName).run()

    elsif arrayRequest.include? "JOIN"

        i = arrayRequest.index("JOIN") + 1
        joinedTable = arrayRequest[i]
        i = arrayRequest.index("ON") + 1
        columnOnDbA = arrayRequest[i].split(".")[1]
        columnOnDbB = arrayRequest[i + 2].split(".")[1]

        request = MySqliteRequest.new().from(tableName).select(columnsName).join(columnOnDbA, joinedTable, columnOnDbB).run()
    else

        request = MySqliteRequest.new().from(tableName).select(columnsName).run()

    end

    print request
    puts ""

end

def readForInsert(buffer) #INSERT

    arrayRequest = bufferSplit(buffer, " ")
    tableName = arrayRequest[2]

    arrayValues = bufferSplit(buffer, "(")
    data = arrayValues[1].chop.split(",")
    
    request = MySqliteRequest.new().values(data).insert(tableName).run()

end

def readForUpdate(buffer) #UPDATE

    arrayRequest = bufferSplit(buffer, " ")
    i = 1
    tableName = arrayRequest[i]

    #hash for set method
    i = arrayRequest.index("SET") + 1
    setHash = Hash.new(0)

    while arrayRequest[i] != "WHERE"

        key = arrayRequest[i]
        value = arrayRequest[i + 2]

        if value.split("").last == ","
            value = value.chop
        end

        value = removeQuotes(value)

        setHash[key] = value
        i = i + 3

    end

    #params for where request
    conditions = readForWhere(arrayRequest)
    conditionField = conditions.keys[0]
    conditionValue = conditions.values[0]

    request = MySqliteRequest.new().from(tableName).where(conditionField, conditionValue).set(setHash).update(tableName).run()

end

def readForDelete(buffer) #DELETE

    arrayRequest = bufferSplit(buffer, " ")
    i = 2
    tableName = arrayRequest[i]

    conditions = readForWhere(arrayRequest)
    conditionField = conditions.keys[0]
    conditionValue = conditions.values[0]
    request = MySqliteRequest.new().from(tableName).where(conditionField, conditionValue).delete().run()

end

def readRequest(buffer)
    if buffer.split.first == "SELECT"

        readForSelect(buffer)

    elsif buffer.split.first == "INSERT"

        readForInsert(buffer)
    
    elsif buffer.split.first == "UPDATE"

        readForUpdate(buffer)

    elsif buffer.split.first == "DELETE"

        readForDelete(buffer)

    else
        print "Wrong request! \n"
    end
end

def readline()

    while buffer = Readline.readline("my_sqlite_cli> ", true)
    
        if buffer == "quit"
            break
        end

        readRequest(buffer)
    end

    return nil
end

readline()