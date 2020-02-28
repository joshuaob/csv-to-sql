# csv-to-sql

A Clojure library designed to import a CSV file into an SQL table. It can be run directly from the command line by passing a path to the CSV. Post-processing of the CSV data can be achieved with post-process methods that take in the raw data and performs a series of operations for the desired output, e.g converting a string date to a valid MySQL date.

## Env

Ensure the following variable are available

DB_NAME_RED_PROD_ICL  
DB_USER_RED_PROD_ICL  
DB_HOST_RED_PROD_ICL  
DB_PASSWORD_RED_PROD_ICL  

## Usage

`lein run "resources/path/to/file.csv"`

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
