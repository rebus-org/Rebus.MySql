# Rebus.MySqlConnector

[![install from nuget](https://img.shields.io/nuget/v/Rebus.MySqlConnector.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.MySqlConnector)

Provides MySQL implementations for [Rebus](https://github.com/rebus-org/Rebus) for

* transport
* sagas
* subscriptions
* timeouts
* saga snapshots
* data bus

Make sure your connection string has 'Allow User Variables=true' for the SQL to work correctly.

**Please note**: This version is using MySqlConnector to connect to MySQL, not the Oracle one. See https://mysqlconnector.net/ for more information.

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---