#!/bin/sh

# configure vo-proxy with password
export LFC_HOST=lfc.cr.cnaf.infn.it
export VO_ATLAS_SW_DIR=/opt/exp_software/atlas
source $VO_ATLAS_SW_DIR/ddm/latest/setup.sh
source /afs/cern.ch/atlas/offline/external/GRID/ddm/DQ2Clients/setup.sh
export DQ2_LOCAL_SITE_ID=INFN-MILANO-ATLASC_LOCALGROUPDISK
echo password | voms-proxy-init -voms atlas -pwstdin

# run dumper
INTERFACE_DIR=../interface
echo python list_d2.sh
echo $? >> $INTERFACE_DIR/xml_list