#!/bin/sh
for n in 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29
do
#	ssh ubuntu@node$n "rm -rf /home/ubuntu/cephtest"
#	[ "$?" = 0 ] && echo node$n cleaned || echo node$n failed 
#	scp ceph.client.admin.keyring  ubuntu@node$n:/etc/ceph 
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
#	ssh ubuntu@node$n "sudo chmod 777 /etc/ceph/ceph.client.admin.keyring"
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
#	scp /etc/ceph/ceph.conf root@node$n:/etc/ceph/
#	ceph osd reweight $n 0.1
	ceph tell osd.$n injectargs  --osd-recovery-delay-start 0
done
exit

for n in 1 2 3 4 5 6 7 8 9 10
do
#	ssh ubuntu@node$n "rm -rf /home/ubuntu/cephtest"
#	[ "$?" = 0 ] && echo node$n cleaned || echo node$n failed 
#	scp ceph.client.admin.keyring  ubuntu@node$n:/etc/ceph 
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
#	ssh ubuntu@node$n "sudo chmod 777 /etc/ceph/ceph.client.admin.keyring"
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
	ssh ubuntu@node$n "sudo sync"
	ssh ubuntu@node$n "sudo reboot"
done

exit

for n in 1 2 3 4 5 6 7 8 9 10
do
#	ssh ubuntu@node$n "rm -rf /home/ubuntu/cephtest"
#	[ "$?" = 0 ] && echo node$n cleaned || echo node$n failed 
#	scp ceph.client.admin.keyring  ubuntu@node$n:/etc/ceph 
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
#	ssh ubuntu@node$n "sudo chmod 777 /etc/ceph/ceph.client.admin.keyring"
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
	ssh ubuntu@node$n "sudo apt-get install ntp"
done
for n in 1 2 3 4 5 6 7 8 9 10
do
#	ssh ubuntu@node$n "rm -rf /home/ubuntu/cephtest"
#	[ "$?" = 0 ] && echo node$n cleaned || echo node$n failed 
#	scp ceph.client.admin.keyring  ubuntu@node$n:/etc/ceph 
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
#	ssh ubuntu@node$n "sudo chmod 777 /etc/ceph/ceph.client.admin.keyring"
#	[ "$?" = 0 ] && echo node$n done || echo node$n failed 
	ssh ubuntu@node$n "ntpq -p"
done
