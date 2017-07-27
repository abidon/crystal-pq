require "spec"
require "../src/pq"

CONNECTION_STRING = "postgres://localhost/postgres?prepared_statements=false"

DB.open CONNECTION_STRING do |conn|
    describe "PQ::ResultSet" do
        describe "#read" do
            it "correctly returns true BOOL" do
                res = conn.scalar("SELECT true")
                res.should be_a(Bool)
                res.should be_true
            end
            
            it "correctly returns false BOOL" do
                res = conn.scalar("SELECT false")
                res.should be_a(Bool)
                res.should be_false
            end

            it "correctly returns BYTEA" do
                # TODO
            end
            
            it "correctly returns CHAR/BPCHAR" do
                res = conn.scalar("SELECT 'a'::char")
                res.should be_a(Char)
                res.should eq 'a'
            end

            it "correctly returns NAME" do
                res = conn.scalar("SELECT 'this is some name'::name")
                res.should be_a(String)
                res.should eq "this is some name"
            end

            it "correctly returns INT8" do
                res = conn.scalar("SELECT 42::bigint")
                res.should be_a(Int64)
                res.should eq 42_i64
            end

            it "correctly returns INT2" do
                res = conn.scalar("SELECT 42::smallint")
                res.should be_a(Int16)
                res.should eq 42_i16
            end

            it "correctly returns INT2VECTOR" do
                res = conn.scalar("SELECT '42 21'::int2vector")
                res.should be_a(Array(Int16))
                res.should eq [42_i32, 21_i32]
            end

            it "correctly returns INT4" do
                res = conn.scalar("SELECT 42::int")
                res.should be_a(Int32)
                res.should eq 42_i32
            end

            it "correctly returns REGPROC" do
                res = conn.scalar("SELECT 'pg_typeof'::regproc")
                res.should be_a(String)
                res.should eq "pg_typeof"
            end

            it "correctly returns TEXT" do
                res = conn.scalar("SELECT 'This is some dummy text'::text")
                res.should be_a(String)
                res.should eq "This is some dummy text"
            end

            it "correctly returns OID" do
                res = conn.scalar("SELECT 42::oid")
                res.should be_a(Int32)
                res.should eq 42_i32
            end

            it "correctly returns OIDVECTOR" do
                res = conn.scalar("SELECT '42 21'::oidvector")
                res.should be_a(Array(Int32))
                res.should eq [42_i32, 21_i32]
            end

            it "correctly returns JSON" do
                res = conn.scalar("SELECT '{\"bool_false\": false, \"bool_true\": true, \"number\": 42.21, \"string\": \"Some string\", \"array\": [false, true, 42.21, \"Some string\"]}'::json")
                res.should be_a JSON::Any
                if res.is_a? JSON::Any
                    res["bool_false"].should eq false
                    res["bool_true"].should eq true
                    res["number"].should eq 42.21
                    res["string"].should eq "Some string"
                    res["array"][0].should eq false
                    res["array"][1].should eq true
                    res["array"][2].should eq 42.21
                    res["array"][3].should eq "Some string"
                else
                    fail "Result is not a JSON::Any"
                end
            end

            it "correctly returns XML" do
                res = conn.scalar("SELECT '<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><data name=\"foo\">bar</data><data name=\"baz\">42</data></root>'::xml")
                res.should be_a XML::Node
                if res.is_a? XML::Node
                    res.first_element_child.should_not be_nil
                    root = res.first_element_child.not_nil!
                    root.name.should eq "root"

                    root.first_element_child.should_not be_nil
                    first_data = root.first_element_child.not_nil!
                    first_data.name.should eq "data"
                    first_data["name"].should eq "foo"
                    first_data.inner_text.should eq "bar"

                    second_data = first_data.next.not_nil!
                    second_data.name.should eq "data"
                    second_data["name"].should eq "baz"
                    second_data.inner_text.should eq "42"
                else
                    fail "Result is not a XML::Node"
                end
            end

            it "correctly returns POINT" do
                res = conn.scalar("SELECT '(42.1,3.14)'::point")
                res.should be_a(PQ::Point)
                res.should eq PQ::Point.new 42.1, 3.14
            end

            it "correctly returns LSEG" do
                res = conn.scalar("SELECT '((42.1,3.14), (21.4,4.31))'::lseg")
                res.should be_a(PQ::Segment)
                res.should eq PQ::Segment.new PQ::Point.new(42.1, 3.14), PQ::Point.new(21.4, 4.31)
            end

            it "correctly returns closed PATH" do
                res = conn.scalar("SELECT '((1,2), (3,4), (5,6))'::path")
                res.should be_a(PQ::Path)
                if res.is_a? PQ::Path
                    res.closed.should eq true
                    res.points[0].should eq PQ::Point.new 1_f64, 2_f64
                    res.points[1].should eq PQ::Point.new 3_f64, 4_f64
                    res.points[2].should eq PQ::Point.new 5_f64, 6_f64
                else
                    fail "Result is not a PQ::Path"
                end
            end

            it "correctly returns open PATH" do
                res = conn.scalar("SELECT '[(1,2), (3,4), (5,6)]'::path")
                res.should be_a(PQ::Path)
                if res.is_a? PQ::Path
                    res.closed.should eq false
                    res.points[0].should eq PQ::Point.new 1_f64, 2_f64
                    res.points[1].should eq PQ::Point.new 3_f64, 4_f64
                    res.points[2].should eq PQ::Point.new 5_f64, 6_f64
                else
                    fail "Result is not a PQ::Path"
                end
            end

            it "correctly returns BOX" do
                res = conn.scalar("SELECT '((1,2), (3,4))'::box")
                res.should be_a(PQ::Box)
                if res.is_a? PQ::Box
                    res.bottom_left.should eq PQ::Point.new 1_f64, 2_f64
                    res.top_right.should eq PQ::Point.new 3_f64, 4_f64
                else
                    fail "Result is not a PQ::Box"
                end
            end

            it "correctly returns closed PATH" do
                res = conn.scalar("SELECT '((1,2), (3,4), (5,6))'::polygon")
                res.should be_a(PQ::Path)
                if res.is_a? PQ::Path
                    res.closed.should eq true
                    res.points[0].should eq PQ::Point.new 1_f64, 2_f64
                    res.points[1].should eq PQ::Point.new 3_f64, 4_f64
                    res.points[2].should eq PQ::Point.new 5_f64, 6_f64
                else
                    fail "Result is not a PQ::Path"
                end
            end

            it "correctly returns LINE" do
                res = conn.scalar("SELECT '{1,2,3}'::line")
                res.should be_a(PQ::Line)
                res.should eq PQ::Line.new 1_f64, 2_f64, 3_f64
            end

            it "correctly returns FLOAT4" do
                res = conn.scalar("SELECT 42.31::float4")
                res.should be_a(Float32)
                res.should eq 42.31_f32
            end

            it "correctly returns FLOAT8" do
                res = conn.scalar("SELECT 42.31::float8")
                res.should be_a(Float64)
                res.should eq 42.31_f64
            end

            it "correctly returns CIRCLE" do
                res = conn.scalar("SELECT '((1,2),3)'::circle")
                res.should be_a(PQ::Circle)
                res.should eq PQ::Circle.new PQ::Point.new(1_f64,2_f64), 3_f64
            end

            it "correctly returns MACADDR" do
                res = conn.scalar("SELECT '01:23:45:67:89:ab'::macaddr")
                res.should be_a(String)
                res.should eq "01:23:45:67:89:ab"
            end

            it "correctly returns INET" do
                res = conn.scalar("SELECT '192.168.0.1/24'::inet")
                res.should be_a(String)
                res.should eq "192.168.0.1/24"
            end

            it "correctly returns CIDR" do
                res = conn.scalar("SELECT '10'::cidr")
                res.should be_a(String)
                res.should eq "10.0.0.0/8"
            end

            it "correctly returns INT2ARRAY" do
                res = conn.scalar("SELECT '{-32768,2,3,4,32767}'::smallint[]")
                res.should be_a(Array(Int16))
                res.should eq [-32768,2,3,4,32767]
            end

            it "correctly returns INT4ARRAY" do
                res = conn.scalar("SELECT '{-2147483648,2,3,4,2147483647}'::int[]")
                res.should be_a(Array(Int32))
                res.should eq [-2147483648,2,3,4,2147483647]
            end

            it "correctly returns TEXTARRAY" do
                res = conn.scalar("SELECT '{\"foo\",\"bar\",\"baz\"}'::text[]")
                res.should be_a(Array(String))
                res.should eq ["foo", "bar", "baz"]
            end

            it "correctly returns OIDARRAY" do
                res = conn.scalar("SELECT '{1,2,3,4}'::oid[]")
                res.should be_a(Array(Int32))
                res.should eq [1,2,3,4]
            end

            it "correctly returns FLOAT4ARRAY" do
                res = conn.scalar("SELECT '{-3.14, 0.0, 3.14}'::float4[]")
                res.should be_a(Array(Float32))
                res.should eq [-3.14_f32, 0_f32, 3.14_f32]
            end

            it "correctly returns FLOAT8ARRAY" do
                res = conn.scalar("SELECT '{-3.14, 0.0, 3.14}'::float8[]")
                res.should be_a(Array(Float64))
                res.should eq [-3.14_f64, 0_f64, 3.14_f64]
            end

            it "correctly returns ACLITEM" do
                res = conn.scalar("SELECT relacl[1] FROM pg_class WHERE relacl IS NOT NULL LIMIT 1")
                res.should be_a(PQ::AclItem)
            end

            it "correctly returns ACLITEMARRAY" do
                res = conn.scalar("SELECT relacl FROM pg_class WHERE relacl IS NOT NULL LIMIT 1")
                res.should be_a(Array(PQ::AclItem))
            end

            it "correctly returns VARCHAR" do
                res = conn.scalar("SELECT 'foo bar baz'::varchar")
                res.should be_a(String)
                res.should eq "foo bar baz"
            end

            it "correctly returns DATE" do
                res = conn.scalar("SELECT '2017-07-27'::date")
                res.should be_a(PQ::Date)
                res.should eq PQ::Date.new 2017_u16, 7_u16, 27_u16
            end
            
            it "correctly returns TIME" do
                res = conn.scalar("SELECT '12:34:56.789123'::time")
                res.should be_a(PQ::Time)
                res.should eq PQ::Time.new 12_u8, 34_u8, 56_u8, 789123_u32, 0
            end
            
            it "correctly returns TIMESTAMP" do
                res = conn.scalar("SELECT '2017-07-27 12:34:56.789123'::timestamp")
                res.should be_a(::Time)
                res.should eq ::Time.new 2017, 7, 27, 12, 34, 56, 789
            end
            
            it "correctly returns TIMESTAMPTZ" do
                res = conn.scalar("SELECT '2017-07-27 12:34:56.789123'::timestamp with time zone")
                res.should be_a(::Time)
                res.should eq ::Time.new 2017, 7, 27, 12, 34, 56, 789
            end
            
            it "correctly returns INTERVAL" do
                res = conn.scalar("SELECT '1 year 2 mons 3 days 04:05:06.789123'::interval")
                res.should be_a(PQ::Interval)
                res.should eq PQ::Interval.new 1,2,3,4,5,6,789123
            end
            
            it "correctly returns TIMETZ" do
                res = conn.scalar("SELECT '12:34:56.789123+02'::time with time zone")
                res.should be_a(PQ::Time)
                res.should eq PQ::Time.new 12_u8, 34_u8, 56_u8, 789123_u32, 2
            end
        end
    end
end