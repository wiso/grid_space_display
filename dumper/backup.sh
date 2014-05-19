#!/bin/sh

cp -r /var/www/html/LOCALGROUPDISK_usage/ ~/bkg_LOCALGROUPDISK_usage/
cd  ~/bkg_LOCALGROUPDISK_usage/; tar cfj LOCALGROUPDISK_usage.tgz LOCALGROUPDISK_usage/*.xml
scp ~/bkg_LOCALGROUPDISK_usage/LOCALGROUPDISK_usage.tgz tier3: