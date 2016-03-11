systemd unit file for btmon
===========================

btmon@.service:  systemd template unit file to define native service for OS
distributions that leverage systemd.

Installation
------------
1. Assume mtools are installed in /usr/local/mtools
2. sudo cp btmon\@.service /etc/systemd/system
3. sudo systemctl daemon-reload

If mtools are installed in a different location then simply edit the paths in the btmon@.service file before copying.

This assumes a user 'btmon' and a group 'btmon' have been created to run this as.  Simply comment the lines 'User=' and 'Group=' to have this run as 'root', which is not recommended.  Alternately to have this run as a different existing user/group simply change the names to your choice.

Use
---
1. For each btmon instance create config file /usr/local/mtools/etc/btmon-_&lt;name&gt;_.cfg
2. For each btmon instance:
    1. sudo systemctl enable btmon@_&lt;name&gt;_
    2. sudo systemctl start btmon@_&lt;name&gt;_

systemd will start each btmon instance as early as possible during boot, and will ensure the instances remain running.  If an instance exits/dies systemd will restart a new instance.

To intentionally stop an individual running instance:
1. sudo systemctl stop btmon@_&lt;name&gt;_
