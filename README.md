# crystal-pq
A "crystal-db"-compliant postgres driver

### Requirements

- libpq

### Usage

* Add the following to your `shards.yml` file:

```yaml
dependencies:
  pq:
    github: abidon/crystal-pq
    branch: master
```

* Open a connection to the database

```crystal
require "db"
require "pq"

DB.open "postgres://localhost:5432/mydb?prepared_statements=false" do |db|
    db.exec "INSERT INTO persons(firstname, lastname) VALUES ($1, $2)", "John", "Doe"
end
```

**Note:** At the moment, only unprepared statements have been implemented, you'll need to add the `?prepared_statements=false` at the end of the connection string to make it work properly.

### Roadmap

* Implement prepared statements
* Support postgres connection string parameters

### Used in...

* [crystal-pq](https://github.com/abidon/crystal-pq): A [crystal-db](https://github.com/crystal-lang/crystal-db) compliant postgres driver

### License

Copyright 2017 Aurélien Bidon (abidon@protonmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
