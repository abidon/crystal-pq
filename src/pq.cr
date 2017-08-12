require "db"
require "json"
require "xml"
require "libpq"

struct PQ::Point
    JSON.mapping({
        x: {type: Float64, setter: false},
        y: {type: Float64, setter: false},
    })

    def initialize(@x : Float64, @y : Float64)
    end
end

struct PQ::Line
    JSON.mapping({
        a: {type: Float64, setter: false},
        b: {type: Float64, setter: false},
        c: {type: Float64, setter: false},
    })

    def initialize(@a : Float64, @b : Float64, @c : Float64)
    end
end

struct PQ::Segment
    JSON.mapping({
        from: {type: PQ::Point, setter: false},
        to: {type: PQ::Point, setter: false},
    })
    
    def initialize(@from : Point, @to : Point)
    end
end

struct PQ::Box
    JSON.mapping({
        top_right: {type: PQ::Point, setter: false},
        bottom_left: {type: PQ::Point, setter: false},
    })

    def initialize(@top_right : Point, @bottom_left : Point)
    end
end

struct PQ::Path
    JSON.mapping({
        points: {type: Array(PQ::Point), setter: false},
        closed: {type: Bool, setter: false},
    })

    getter points
    getter closed
    def initialize(@points : Array(Point), @closed : Bool = true)
    end
end

struct PQ::Circle
    JSON.mapping({
        center: {type: PQ::Point, setter: false},
        radius: {type: Float64, setter: false},
    })

    def initialize(@center : Point, @radius : Float64)
    end
end

struct PQ::MacAddress
    def self.parse(s : String)
        return self.new s
    end

    protected def initialize(@s : String)
    end

    def to_json(builder)
        builder.scalar(@s)
    end
end

struct PQ::AclItem
    def initialize(@s : String)
    end

    def to_json(builder)
        builder.scalar(@s)
    end
end

struct PQ::Date
    def self.parse(s : String)
        splitted = s.split('-').map {|v| v.to_u16}
        self.new splitted[0], splitted[1], splitted[2]
    end

    def initialize(@year : UInt16, @month : UInt16, @day : UInt16)
    end

    def iso8601
        "#{@year.to_s.rjust(4, '0')}-#{@month.to_s.rjust(2, '0')}-#{@day.to_s.rjust(2, '0')}"
    end
    
    def to_json(builder)
        builder.scalar(iso8601)
    end
end

struct PQ::Time
    def self.parse(s : String)
        splitted = s.split(/[:\.+]/).map {|v| v.to_i64}
        self.new splitted[0].to_u8, splitted[1].to_u8, splitted[2].to_u8, splitted[3].to_u32, (splitted[4]? || 0).to_i32
    end

    def initialize(@hour : UInt8, @minutes : UInt8, @seconds : UInt8, @micros : UInt32, @tz : Int32)
    end

    def iso8601
        micros = @micros == 0 ? "" : ".#{@micros.to_s.rjust(6, '0')}"
        tz = "+#{@tz.to_s.rjust(4, '0')}"
        "T#{@hout.to_s.rjust(2, '0')}-#{@minutes.to_s.rjust(2, '0')}-#{@seconds.to_s.rjust(2, '0')}#{micros}"
    end
    
    def to_json(builder)
        builder.scalar(iso8601)
    end
end

struct PQ::Interval
    def initialize(@years : Int32, @months : Int32, @days : Int32, @hours : Int32, @minutes : Int32, @seconds : Int32, @micros : Int32)
    end

    def iso8601
        micros = @micros == 0 ? "" : ".#{@micros.to_s.rjust(6, '0')}"
        return "P#{@years}Y#{@months}M#{@days}DT#{@hours}H#{@minutes}M#{@seconds}#{micros}S"
    end
    
    def to_json(builder)
        builder.scalar(iso8601)
    end
end

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
                DB::ExecResult.new (String.new(LibPQ.cmd_tuples result).to_i64 rescue LibPQ.ntuples(result).to_i64), 0_i64
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
        v, ri, ci = self.read_raw

        if v.nil?
            return nil
        else
            v = v.not_nil!
        end

        case LibPQ::TypeOid.new LibPQ.ftype(@result, ci)
        when LibPQ::TypeOid::BOOL
            return v == "t" ? true : false
        when LibPQ::TypeOid::BYTEA # TODO
        when LibPQ::TypeOid::CHAR, LibPQ::TypeOid::BPCHAR
            return v[0]
        when LibPQ::TypeOid::REGPROC, LibPQ::TypeOid::TEXT, LibPQ::TypeOid::VARCHAR, LibPQ::TypeOid::NAME, LibPQ::TypeOid::MACADDR, LibPQ::TypeOid::INET, LibPQ::TypeOid::CIDR, LibPQ::TypeOid::UNKNOWN, LibPQ::TypeOid::UUID
            return v
        when LibPQ::TypeOid::INT8
            return v.to_i64
        when LibPQ::TypeOid::INT4, LibPQ::TypeOid::OID, LibPQ::TypeOid::TID, LibPQ::TypeOid::XID, LibPQ::TypeOid::CID
            return v.to_i32
        when LibPQ::TypeOid::INT2
            return v.to_i16
        when LibPQ::TypeOid::INT2VECTOR
            return v.split(' ').map { |i| i.to_i16 }
        when LibPQ::TypeOid::OIDVECTOR
            return v.split(' ').map { |i| i.to_i32 }
        when LibPQ::TypeOid::JSON
            return JSON.parse v
        when LibPQ::TypeOid::XML
            return XML.parse v
        when LibPQ::TypeOid::PGNODETREE # TODO
        when LibPQ::TypeOid::PGDDLCOMMAND # TODO
        when LibPQ::TypeOid::POINT
            return self.parse_point(v)
        when LibPQ::TypeOid::LSEG
            # format: [(x1,y1),(x2,y2)]
            first_parenthese = v.index(')').not_nil!
            second_parenthese = v.index(')', first_parenthese + 1).not_nil!
            from = self.parse_point(v[1..first_parenthese])
            to = self.parse_point(v[first_parenthese+2..second_parenthese])
            return Segment.new from, to
        when LibPQ::TypeOid::PATH, LibPQ::TypeOid::POLYGON
            points = Array(Point).new
            offset = 0
            while true
                match? = /(\([0-9]*(\.[0-9]*)?,[0-9]*(\.[0-9]*)?\))/.match(v, offset)
                break if match?.nil?
                match = match?.not_nil!
                points << self.parse_point(match[0])
                offset = match.end(0).not_nil!
            end
            return Path.new points, v[0] == '('
        when LibPQ::TypeOid::BOX
            # format: (x1,y1),(x2,y2)
            first_parenthese = v.index(')').not_nil!
            second_parenthese = v.index(')', first_parenthese + 1).not_nil!
            from = self.parse_point(v[0..first_parenthese])
            to = self.parse_point(v[first_parenthese+2..second_parenthese])
            return Box.new from, to
        when LibPQ::TypeOid::LINE
            # format: {a,b,c}
            var = v[1..-2].split(',').map {|i| i.to_f64}.to_a

            return Line.new var[0], var[1], var[2]
        when LibPQ::TypeOid::FLOAT4
            return v.to_f32
        when LibPQ::TypeOid::FLOAT8
            return v.to_f64
        when LibPQ::TypeOid::ABSTIME, LibPQ::TypeOid::RELTIME # TODO but legacy
        when LibPQ::TypeOid::TINTERVAL # TODO
        when LibPQ::TypeOid::CIRCLE
            closing_parenthese = v.index(')').not_nil!
            center = self.parse_point(v[1..closing_parenthese])
            radius = v[closing_parenthese+2..-2].to_f64
            return Circle.new center, radius
        when LibPQ::TypeOid::CASH # TODO
        when LibPQ::TypeOid::INT2ARRAY
            return v[1..-2].split(',').map {|v| v.to_i16}
        when LibPQ::TypeOid::INT4ARRAY, LibPQ::TypeOid::OIDARRAY
            return v[1..-2].split(',').map {|v| v.to_i32}
        when LibPQ::TypeOid::TEXTARRAY
            # Note: without binary form, text containing commas are splitted as different entries in the output array
            return v[1..-2].split(',')
        when LibPQ::TypeOid::FLOAT4ARRAY
            return v[1..-2].split(',').map {|v| v.to_f32}
        when LibPQ::TypeOid::FLOAT8ARRAY
            return v[1..-2].split(',').map {|v| v.to_f64}
        when LibPQ::TypeOid::ACLITEM
            return AclItem.new v
        when LibPQ::TypeOid::ACLITEMARRAY
            return v[1..-2].split(',').map {|v| AclItem.new v}
        when LibPQ::TypeOid::CSTRINGARRAY # TODO
        when LibPQ::TypeOid::DATE
            return Date.parse v
        when LibPQ::TypeOid::TIME, LibPQ::TypeOid::TIMETZ
            return Time.parse v
        when LibPQ::TypeOid::TIMESTAMP
            return (::Time.parse v, "%F %X") + ::Time::Span.new 0,0,0,0, v.split('.')[1].to_i32/1000
        when LibPQ::TypeOid::TIMESTAMPTZ
            splitted = v.split(/[+\.]/)
            return (::Time.parse "#{splitted[0]}+#{splitted[2]}00", "%F %X+%z") + ::Time::Span.new 0,0,0,0, splitted[1].to_i32/1000
        when LibPQ::TypeOid::INTERVAL
            rgx = /(([+-]?\d+)\s+years?)?\s*(([+-]?\d+)\s+mons?)?\s*(([+-]?\d+)\s+days?)?\s*(([+-])?([\d]*):(\d\d):(\d\d)\.?(\d{1,6})?)?/

            match? = rgx.match(v)
            if m = match?
                return Interval.new(
                    safe_get(m,2), safe_get(m,4), safe_get(m,6),
                    safe_get(m,9), safe_get(m,10), safe_get(m,11), safe_get(m,12)
                )
            else
                raise Exception.new "Invalid format for #{LibPQ::TypeOid.new LibPQ.ftype(@result, ci)} typed '#{v}'"
            end
        when LibPQ::TypeOid::BIT # TODO
        when LibPQ::TypeOid::VARBIT # TODO
        when LibPQ::TypeOid::NUMERIC # TODO
        when LibPQ::TypeOid::REFCURSOR # TODO
        when LibPQ::TypeOid::REGPROCEDURE # TODO
        when LibPQ::TypeOid::REGOPER # TODO
        when LibPQ::TypeOid::REGOPERATOR # TODO
        when LibPQ::TypeOid::REGCLASS # TODO
        when LibPQ::TypeOid::REGTYPE # TODO
        when LibPQ::TypeOid::REGROLE # TODO
        when LibPQ::TypeOid::REGNAMESPACE # TODO
        when LibPQ::TypeOid::REGTYPEARRAY # TODO
        when LibPQ::TypeOid::LSN # TODO
        when LibPQ::TypeOid::TSVECTOR # TODO
        when LibPQ::TypeOid::GTSVECTOR # TODO
        when LibPQ::TypeOid::TSQUERY # TODO
        when LibPQ::TypeOid::REGCONFIG # TODO
        when LibPQ::TypeOid::REGDICTIONARY # TODO
        when LibPQ::TypeOid::JSONB # TODO
        when LibPQ::TypeOid::INT4RANGE # TODO
        when LibPQ::TypeOid::RECORD # TODO
        when LibPQ::TypeOid::RECORDARRAY # TODO
        when LibPQ::TypeOid::CSTRING # TODO
        when LibPQ::TypeOid::ANY # TODO
        when LibPQ::TypeOid::ANYARRAY # TODO
        when LibPQ::TypeOid::VOID # TODO
        when LibPQ::TypeOid::TRIGGER # TODO
        when LibPQ::TypeOid::EVTTRIGGER # TODO
        when LibPQ::TypeOid::LANGUAGE_HANDLER # TODO
        when LibPQ::TypeOid::INTERNAL # TODO
        when LibPQ::TypeOid::OPAQUE # TODO
        when LibPQ::TypeOid::ANYELEMENT # TODO
        when LibPQ::TypeOid::ANYNONARRAY # TODO
        when LibPQ::TypeOid::ANYENUM # TODO
        when LibPQ::TypeOid::FDW_HANDLER # TODO
        when LibPQ::TypeOid::INDEX_AM_HANDLER # TODO
        when LibPQ::TypeOid::TSM_HANDLER # TODO
        when LibPQ::TypeOid::ANYRANGE # TODO
        end

        raise Exception.new "Unknown type OID: #{LibPQ::TypeOid.new LibPQ.ftype(@result, ci)} for value '#{v}'"
    end

    def read(t : UInt8.class) : UInt8
      value = read(Int).to_u8
    end

    def read(t : UInt16.class) : UInt16
      value = read(Int).to_u16
    end

    def read(t : UInt32.class) : UInt32
      value = read(Int).to_u32
    end

    def read(t : UInt64.class) : UInt64
      value = read(Int).to_u64
    end

    def read_raw : Tuple(String?, Int64, Int32)
        if @column_index >= @column_count
            raise Exception.new "[PQ_Error]: Unable to read value for out-of-range column !"
        end

        begin
            if LibPQ.get_is_null(@result, @row_index, @column_index) == 1
                return nil, @row_index, @column_index
            else
                return String.new(LibPQ.get_value @result, @row_index, @column_index), @row_index, @column_index
            end
        ensure
            @column_index = @column_index + 1
        end
    end

    def safe_get(m, i)
        begin
            return m[i].to_i32
        rescue
            return 0
        end
    end

    def parse_point(s : String) : Point
        # format: (x,y)
        coord = s[1..-2].split(',').map { |f| f.to_f64 }
        return Point.new coord[0], coord[1]
    end
end

DB.register_driver "postgres", PQ::Driver
