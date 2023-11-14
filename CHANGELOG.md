# Changelog

## 1.0.0
* Initial version - thanks [mvandevy]
* Fix csproj - thanks [robvanpamel]
* Fix table creation at startup - thanks [robvanpamel]

## 1.1.0
* Update MySql.Data dep to 8.0.12 - thanks [robvanpamel]

## 1.1.1
* Fix bug in table inspection - thanks [renemadsen]

## 1.1.2
* Remove misleading parameter names

## 2.0.1
* Update to Rebus 6

## 3.0.0-b1
* New MySQL persistence based on Rebus.SqlServer - thanks [kendallb]

## 3.0.0-b2
* Ported to open source MySqlConnector which is faster, fully async and under a better license - thanks [kendallb]
* Removed obsolete configuration functions ported from SQL Server - thanks [kendallb]
* Performance improvements for MySQL 5.7 by mixing partial async  - thanks [kendallb]for reads - thanks [kendallb]
* Removed Async bottleneck which is not really needed - thanks [kendallb]

## 3.0.0-b3
* Remove MSSQL-specific index limitations - thanks [kendallb]

## 3.0.0-b8
* Removed leased based transport and made it the default due to how MySQL works with row locking - thanks [kendallb]
* Change transaction isolation mode to REPEATABLE READ - thanks [kendallb]
* Add distributed saga exclusive locks - thanks [kendallb]
* Update Rebus dep to 7 prerelease - thanks [kendallb]
* Avoid MySQL access during startup if not creating tables - thanks [kendallb]

## 4.0.0-alpha02
* Update MySqlConnector to 2.2.5
* Update to Rebus 8
* Update MySql.Data to 8.0.33 - thanks [huysentruitw]


[huysentruitw]: https://github.com/huysentruitw
[kendallb]: https://github.com/kendallb
[mvandevy]: https://github.com/mvandevy
[renemadsen]: https://github.com/renemadsen
[robvanpamel]: https://github.com/robvanpamel
