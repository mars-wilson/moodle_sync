<!-- file: README.md -->

# moodle_sync
Moodle Synchronization Tools in Python

This project provides tools for synchronizing Moodle courses and enrolments 
to and from an external data source or another Moodle instance.

It comes with an extensible architecture. 

# Notes

Data elements from views and sources are always converted to a 
Moodle-ready format,
and in general, moodle fields (column names) are used
when pulling data from other data sources.

Of particular difficulty is converting dates from a SQL database
into Unix timestamps in a way that doesn't get altered by timezones
and messed up because of changes in dayloight savings time between
course start dates and end dates.  Those operations are best done in Python, 
and an example MSSQL provider
shows how you might go about that conversion.

# Examples

There is one example shown for synchronizing courses to Moodle
from a Jenzabar J1 ERP system.  This example uses MSSQL and the Moodle API.

# Installation
Should be able to just do pip install

# Configuration
Configuration is done in a config file.  
See the example in the examples directory.
You can set the config values after importing as you wish.
Logging by default uses a safe print funciton in th the util module
that attempts to mask sensitive credentials and tokens.

You can also override any of the provided classes, of course,
to do things your way, and then use the base features as needed.

# To Do
* Make CSV providers that can push CSVs via SSH to a Moodle server
so that the Moodle file sync can source it.
* Make a custom enrollment plugin for Moodle that the API can then
sync to.




