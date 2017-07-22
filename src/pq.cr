require "db"
require "libpq"

class PQ::Driver < DB::Driver
    def build_connection(context : DB::ConnectionContext)
        PQ::Connection.new context
    end
end

class PQ::Connection < DB::Connection
    @conn : LibPQ::Conn*
    getter conn

    def initialize(context : DB::ConnectionContext)
        super(context)

        conn_uri = context.uri.dup
        conn_uri.query = nil

        @conn = LibPQ.connect_db(conn_uri.to_s.to_unsafe)

        if LibPQ.status(@conn) == LibPQ::ConnStatusType::CONNECTION_BAD
            LibPQ.finish(@conn)
            raise DB::ConnectionRefused.new
        end
    end

    def do_close
        super
        LibPQ.finish(@conn)
    end

    def build_prepared_statement(query) : DB::Statement
        PQ::Statement.new self, query
    end

    def build_unprepared_statement(query) : DB::Statement
        PQ::UnpreparedStatement.new self, query
    end
end

class PQ::Statement < DB::Statement
    def initialize(connection : PQ::Connection, @sql : String)
        # TODO
        super connection
    end

    def perform_query(args : Enumerable) : DB::ResultSet
        # TODO
        result = LibPQ.make_empty_result @connection.as(PQ::Connection).conn, LibPQ::ExecStatusType::TUPLES_OK
        return PQ::ResultSet.new self, result
    end

    def perform_exec(args : Enumerable) : DB::ExecResult
        # TODO
        DB::ExecResult.new -1_i64, -1_i64
    end
end

class PQ::UnpreparedStatement < DB::Statement
    def initialize(connection : PQ::Connection, @sql : String)
        super connection
    end

    def perform_query(args : Enumerable) : DB::ResultSet
        if args.size == 0
            casted_args = StaticArray(UInt8*, 0).new Pointer(UInt8).null
        else
            args = args.map { |arg| arg.to_s }
            casted_args = args.map { |arg| arg.to_unsafe }.to_a
            if casted_args.is_a?(Array(NoReturn))
                casted_args = Array(UInt8*).new 0
            end
        end

        result = LibPQ.exec_params @connection.as(PQ::Connection).conn, @sql, args.size, Pointer(LibPQ::Oid).null, casted_args.to_unsafe, Pointer(Int32).null, Pointer(Int32).null, 0
        status = LibPQ.result_status result
        if status == LibPQ::ExecStatusType::COMMAND_OK || status == LibPQ::ExecStatusType::TUPLES_OK
            return PQ::ResultSet.new self, result 
        else
            raise Exception.new "[Error] #{status}: #{String.new LibPQ.error_message @connection.as(PQ::Connection).conn}"
        end
    end

    def perform_exec(args : Enumerable) : DB::ExecResult
        if args.size == 0
            casted_args = StaticArray(UInt8*, 0).new Pointer(UInt8).null
        else
            args = args.map { |arg| arg.to_s }
            casted_args = args.map { |arg| arg.to_unsafe }.to_a
            if casted_args.is_a?(Array(NoReturn))
                casted_args = Array(UInt8*).new 0
            end
        end

        begin
            result = LibPQ.exec_params @connection.as(PQ::Connection).conn, @sql, args.size, Pointer(LibPQ::Oid).null, casted_args.to_unsafe, Pointer(Int32).null, Pointer(Int32).null, 0
            status = LibPQ.result_status result
            if status == LibPQ::ExecStatusType::COMMAND_OK
                DB::ExecResult.new String.new(LibPQ.cmd_tuples result).to_i64, 0_i64
            elsif status == LibPQ::ExecStatusType::TUPLES_OK
                affected_rows = String.new(LibPQ.cmd_tuples result).to_i64
                if LibPQ.nfields(result) == 1 && affected_rows > 0
                    last_inserted_id = String.new(LibPQ.get_value(result, affected_rows - 1, 0)).to_i64 rescue 0_i64
                else
                    last_inserted_id = 0_i64
                end
                DB::ExecResult.new affected_rows, last_inserted_id
            else
                raise Exception.new "[PQ_Error] #{status}: #{String.new LibPQ.error_message @connection.as(PQ::Connection).conn}"
            end
        ensure
            LibPQ.clear result
        end
    end
end

class PQ::ResultSet < DB::ResultSet
    @column_count : Int32
    @column_index : Int32 = 0
    @row_count : Int64
    @row_index : Int64 = -1_i64

    def initialize(statement, @result : LibPQ::Result*)
        # TODO
        super statement
        @column_count = LibPQ.nfields(@result)
        @row_count = String.new(LibPQ.cmd_tuples result).to_i64
    end

    getter column_count

    def column_name(index : Int32) : String
        String.new LibPQ.fname(@result, index)
    end

    def do_close
        super
        LibPQ.clear @result
    end

    def move_next : Bool
        if @row_index >= @row_count - 1
            return false
        end

        @column_index = 0
        @row_index = @row_index + 1

        return true
    end

    def read
        if @column_index >= @column_count
            raise Exception.new "[PQ_Error]: Unable to read value for out-of-range column !"
        end

        begin
            if LibPQ.get_is_null(@result, @row_index, @column_index) == 1
                return nil
            else
                return String.new LibPQ.get_value @result, @row_index, @column_index
            end
        ensure
            @column_index = @column_index + 1
        end
    end
end

DB.register_driver "postgres", PQ::Driver
