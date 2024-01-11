# Bully Algorithm Coding Project

## Description

This python project was created to emulate the distributed systems bully algorithm. In this algorithm the highest ID process is the coordinator. If a process thinks the coordinator has failed an election is sent out to higher number processes and if they acknowledge the lower numbered process is out of the election. Then the highest number process that has acknowledged the election will send a coordination message out becoming the coordinator.
