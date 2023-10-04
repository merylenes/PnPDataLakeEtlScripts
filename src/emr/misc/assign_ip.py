#!/usr/bin/python3
#
#Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file 
#except in compliance with the License. A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
#or in the "license" file accompanying this file. This file is distributed on an "AS IS" 
#BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
#License for the specific language governing permissions and limitations under the License.

# This script was hacked from the original script on github.
# The main difference here is that we work out the region we're in (duh, that's in the meta-data so why pass it 
#       in as a parameter to this script. I can't think of a scenario where the region would be different)
# Also, we assign an IP address and give it a description of emr-master-ip.
# This script will look for that Description and also print to stdout the relevant info we determine

import sys, subprocess

is_master = subprocess.check_output(['cat /emr/instance-controller/lib/info/instance.json | jq .isMaster'], shell=True).strip().decode('ascii')

if is_master == 'true':

    #Configure iptablles rules such that traffic is redirected from the secondary to the primary IP address:
    primary_ip = subprocess.check_output(['/usr/bin/curl -s http://169.254.169.254/latest/meta-data/local-ipv4'], shell=True, universal_newlines=True).strip()
    print(f"Primary EMR IP: {primary_ip}")

    region = subprocess.check_output(['/usr/bin/curl -s http://169.254.169.254/latest/dynamic/instance-identity/document|jq -r ".region"'], shell=True).strip().decode('ascii')
    print(f"Region: {region}")
    instance_id = subprocess.check_output(['/usr/bin/curl -s http://169.254.169.254/latest/meta-data/instance-id'], shell=True).decode('ascii')
    print(f"InstanceId: {instance_id}")
    
    interface_cmd = f"aws ec2 describe-instances --region {region} --instance-ids {instance_id} | jq -r '.Reservations[].Instances[].NetworkInterfaces[].NetworkInterfaceId'"
    interface_id = subprocess.check_output([interface_cmd], shell=True).strip().decode('ascii').strip('"')
    print(f"Interface Id: {interface_id}")

    #Assign private IP to the master instance:
    emr_master_ip = "aws ec2 describe-network-interfaces --query \"NetworkInterfaces[?Description == 'emr-master-ip'].PrivateIpAddress\"|jq -r '.[0]'"
    private_ip = subprocess.check_output([emr_master_ip], shell=True).strip().decode('ascii')
    print(f"EMR Private IP: {private_ip}")

    #assign_ip = f"aws ec2 attach-network-interface --network-interface-id {interface_id} --instance-id {instance_id} --device-index 1"
    #private_ip_cmd = f"aws ec2 assign-private-ip-addresses --region {region} --network-interface-id {interface_id} --private-ip-addresses {private_ip}"
    #priv_ip_return = subprocess.check_call([private_ip_cmd], shell=True)

    subnet_cmd = f"aws ec2 describe-instances --region {region} --instance-ids {instance_id} | jq .Reservations[].Instances[].NetworkInterfaces[].SubnetId"
    subnet_ids = subprocess.check_output([subnet_cmd], shell=True).strip().decode('ascii').strip('"').strip().strip('"')
    print(f"Subnet ID: {subnet_ids}")

    subnet_cidr_cmd =  f"aws ec2 describe-subnets --region {region} --subnet-ids {subnet_ids} | jq .Subnets[].CidrBlock"
    subnet_cidr = subprocess.check_output([subnet_cidr_cmd], shell=True).strip().decode('ascii').strip('"')
    print(f"Subnet CIDR: {subnet_cidr}")

    cidr_prefix = subnet_cidr.split("/")[1]
    print(f"CIDR prefix: {cidr_prefix}")

    #Add the private IP address to the default network interface:
    attach_ip_to_iface_cmd = f"sudo ip addr add dev eth0 {private_ip}/{cidr_prefix}"
    subprocess.check_call([attach_ip_to_iface_cmd], shell=True)

    nat_ip_cmd = f"sudo iptables -t nat -A PREROUTING -d {primary_ip} -j DNAT --to-destination {private_ip}"
    subprocess.check_call([nat_ip_cmd], shell=True)

    host_cmd = f"/usr/bin/host {private_ip}"
    rev_dns_record = subprocess.check_output([host_cmd], shell=True).strip().decode('ascii')
    print(f"\nhost rev record: {rev_dns_record}")

    #resolvctl = f"/usr/bin/resolvectl status"
    #resolvectl_output = subprocess.check_output([resolvctl], shell=True).strip().decode('ascii')
    #print(resolvectl_output)

    #fwd_lookup = rev_dns_record.split(" ")[4].strip(".")
    #print(f"going to lookup fwd record for: {fwd_lookup}")
    #host_cmd = f"/usr/bin/host {fwd_lookup}"
    #fwd_dns_record = subprocess.check_output([host_cmd], shell=True).strip().decode('ascii')
    #print(f"host fwd record: {fwd_dns_record}")
    #print(f"="*30)

    show_interfaces = f"sudo ip addr list dev eth0"
    print("Interfaces on eth0 are now as follows:\n")
    print(subprocess.check_output([show_interfaces], shell=True).strip().decode('ascii'))

else:
    print("Not the master node")


#import sys, subprocess
#
#is_master = subprocess.check_output(['cat /emr/instance-controller/lib/info/instance.json | jq .isMaster'], shell=True).strip().decode('ascii')
#
#if is_master == "true":
#    private_ip = str(sys.argv[1])
#    region = str(sys.argv[2])
#    instance_id = subprocess.check_output(['/usr/bin/curl -s http://169.254.169.254/latest/meta-data/instance-id'], shell=True).decode('ascii')
#    aws_cmd = f"aws ec2 describe-instances --region {region} --instance-ids {instance_id} | jq .Reservations[].Instances[].NetworkInterfaces[].NetworkInterfaceId"
#    interface_id = subprocess.check_output([aws_cmd], shell=True).strip().decode('ascii').strip('"')
#
#    print(f"{private_ip}, {instance_id}, {interface_id}")
