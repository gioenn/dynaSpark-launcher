import os
import base64
import binascii
import time

from libcloud.common.types import LibcloudError

from libcloud.compute.drivers.azure_arm import AzureNodeDriver, AzureNic, AzureNetwork, NodeAuthSSHKey, \
    NodeAuthPassword, AzureSubnet, AzureVhdImage, AzureImage
from libcloud.compute.providers import set_driver


def set_azurearm_driver():
    """ Convenience method to set the custom driver"""
    driver_module = "drivers.azurearm.driver"
    driver_klass = "CustomAzureNodeDriver"
    driver_provider = "CustomAzureArm"
    try:
        set_driver(driver_provider, driver_module, driver_klass)
    except AttributeError as e:  # usually raised when AzureArm is already registered
        print(str(e))


class AzureResourceGroup(object):
    """Represent an Azure Resource Group"""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return (('<AzureResourceGroup: id=%s, name=%s ...>')
                % (self.id, self.name))


class AzureStorageAccount(object):
    """Represent an Azure Storage Account"""

    def __init__(self, id, name, location, sku, kind, extra):
        self.id = id
        self.name = name
        self.location = location
        self.sku = sku
        self.kind = kind
        self.extra = extra

    def __repr__(self):
        return (('<AzureStorageAccount: id=%s, name=%s ...>')
                % (self.id, self.name))


class AzureSecurityGroup(object):
    """Represents an Azure Security Group"""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return (('<AzureSecurityGroup: id=%s, name=%s ...>')
                % (self.id, self.name))


class CustomAzureNodeDriver(AzureNodeDriver):
    """ Custom AzureARM node driver, created to overcome limitation on libcloud's driver"""

    # def ex_create_public_ip(self, name, resource_group, location=None):
    #     """
    #     Create a public IP resources.
    #
    #     :param name: Name of the public IP resource
    #     :type name: ``str``
    #
    #     :param resource_group: The resource group to create the public IP
    #     :type resource_group: ``str``
    #
    #     :param location: The location at which to create the public IP
    #     (if None, use default location specified as 'region' in __init__)
    #     :type location: :class:`.NodeLocation`
    #
    #     :return: The newly created public ip object
    #     :rtype: :class:`.AzureIPAddress`
    #     """
    #
    #     if location is None and self.default_location:
    #         location = self.default_location
    #     else:
    #         raise ValueError("location is required.")
    #
    #     target = "/subscriptions/%s/resourceGroups/%s/" \
    #              "providers/Microsoft.Network/publicIPAddresses/%s" \
    #              % (self.subscription_id, resource_group, name)
    #     data = {
    #         "location": location.id,
    #         "tags": {},
    #         "properties": {
    #             "publicIPAllocationMethod": "Dynamic"
    #         }
    #     }
    #     r = self.connection.request(target,
    #                                 params={"api-version": "2015-06-15"},
    #                                 data=data,
    #                                 method='PUT'
    #                                 )
    #     return self._to_ip_address(r.object)

    def ex_create_network_interface(self, name, subnet, resource_group,
                                    location=None, public_ip=None):
        """
        Create a virtual network interface (NIC).

        :param name: Name of the NIC resource
        :type name: ``str``

        :param subnet: The subnet to attach the NIC
        :type subnet: :class:`.AzureSubnet`

        :param resource_group: The resource group to create the NIC
        :type resource_group: ``str``

        :param location: The location at which to create the NIC
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param public_ip: Associate a public IP resource with this NIC
        (optional).
        :type public_ip: :class:`.AzureIPAddress`

        :return: The newly created NIC
        :rtype: :class:`.AzureNic`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = "/subscriptions/%s/resourceGroups/%s/providers" \
                 "/Microsoft.Network/networkInterfaces/%s" \
                 % (self.subscription_id, resource_group, name)

        data = {
            "location": location.id,
            "tags": {},
            "properties": {
                "ipConfigurations": [{
                    "name": "myip1",
                    "properties": {
                        "subnet": {
                            "id": subnet.id
                        },
                        "privateIPAllocationMethod": "Dynamic"
                    }
                }]
            }
        }

        if public_ip:
            data["properties"]["ipConfigurations"][0]["properties"]["publicIPAddress"] = {
                "id": public_ip.id
            }

        r = self.connection.request(target,
                                    params={"api-version": "2015-06-15"},
                                    data=data,
                                    method='PUT'
                                    )
        while r.object is None:
            time.sleep(1)

        return AzureNic(r.object["id"], r.object["name"], r.object["location"],
                        r.object["properties"])

    def ex_create_resource_group(self, resource_group, location=None):
        """Create a ResourceGroup

        :param resource_group: name of the resource group
        :param location: location of the resource group
        :return:
        """
        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = "/subscriptions/%s/resourcegroups/%s" % (self.subscription_id, resource_group)
        params = {'api-version': '2016-09-01'}
        data = {
            "location": location.id,
            "tags": {},
        }

        r = self.connection.request(action=target,
                                    params=params,
                                    data=data,
                                    method="PUT")

        while r.object is None:
            time.sleep(1)

        return AzureResourceGroup(r.object["id"], r.object["name"], r.object["location"], r.object["properties"])

    def ex_create_storage_account(self, resource_group, storage_account, sku='standard_lrs', kind='storage', extra=None,
                                  location=None):
        """Create a StorageAccount

        :param resource_group: name of the resource group
        :param storage_account: name of the storage account
        :param sku: ‘Standard_LRS’, ‘Standard_GRS’, ‘Standard_RAGRS’, ‘Standard_ZRS’, ‘Premium_LRS’
        :param kind: 'Storage', 'BlobStorage'
        :param extra: extra content for properties field
        :param location: location of the storage account
        :return:
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Storage/storageAccounts/%s" % (
            self.subscription_id, resource_group, storage_account)
        params = {"api-version": "2016-12-01"}
        data = {
            'sku': {'name': sku},
            'kind': kind,
            'location': location.id,
            'tags': {}
        }

        if extra:
            data['properties'] = extra

        r = self.connection.request(action=target,
                                    params=params,
                                    data=data,
                                    method="PUT")

        while r.object is None:
            time.sleep(1)
        if type(r.object) is str:
            print(r.object)
        return AzureStorageAccount(r.object["id"], r.object["name"], r.object["location"], r.object["sku"],
                                   r.object["kind"], r.object["properties"])

    def ex_create_security_group(self, resource_group, security_group, security_rules=None, location=None):
        """Create a security group

        :param resource_group: name of the resource group
        :param security_group: name of the security group
        :param security_rules: list of security rules
        :param location: location of the security group
        :return:
        """
        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/networkSecurityGroups/%s" % (
            self.subscription_id, resource_group, security_group)
        params = {"api-version": "2016-09-01"}
        data = {
            'location': location.id,
            'tags': {}
        }

        if security_rules:
            data.update({'properties': {'securityRules': security_rules}})

        r = self.connection.request(action=target,
                                    params=params,
                                    data=data,
                                    method="PUT")

        while r.object is None:
            time.sleep(1)

        return AzureSecurityGroup(r.object["id"], r.object["name"], r.object["location"], r.object["properties"])

    def ex_create_network(self, resource_group, network, extra=None, location=None):
        """ Create a new network. Subnets can be created using the "properties" in extra field

        :param resource_group: name of the resource group
        :param network: name of the network
        :param extra: extra properties
        :param location: location of the network
        :return:
        """
        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")
        target = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/virtualNetworks/%s" % (
            self.subscription_id, resource_group, network)
        params = {"api-version": "2016-03-30"}
        data = {
            "tags": {},
            "location": location.id,
        }

        if extra:
            data["properties"] = extra

        r = self.connection.request(action=target,
                                    params=params,
                                    data=data,
                                    method="PUT")

        while r.object is None:
            time.sleep(1)

        return AzureNetwork(r.object["id"], r.object["name"], r.object["location"], r.object["properties"])

    def ex_destroy_resource_group(self, resource_group, location=None):
        """Destroy a ResourceGroup and all its content

        :param resource_group: the resource group to be destroyed
        :param location: location of the resource group
        :return:
        """
        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = "/subscriptions/%s/resourcegroups/%s" % (self.subscription_id, resource_group)
        params = {'api-version': '2016-09-01'}

        r = self.connection.request(action=target,
                                    params=params,
                                    method="DELETE")

        return

    def create_node(self,
                    name,
                    size,
                    image,
                    auth,
                    ex_resource_group,
                    ex_storage_account,
                    ex_blob_container="vhds",
                    location=None,
                    ex_user_name="azureuser",
                    ex_network=None,
                    ex_subnet=None,
                    ex_nic=None,
                    ex_tags={},
                    ex_customdata=""):
        """Create a new node instance. This instance will be started
        automatically.

        This driver supports the ``ssh_key`` feature flag for ``created_node``
        so you can upload a public key into the new instance::

            >>> from libcloud.compute.drivers.azure_arm import AzureNodeDriver
            >>> driver = AzureNodeDriver(...)
            >>> auth = NodeAuthSSHKey('pubkey data here')
            >>> node = driver.create_node("test_node", auth=auth)

        This driver also supports the ``password`` feature flag for
        ``create_node``
        so you can set a password::

            >>> driver = AzureNodeDriver(...)
            >>> auth = NodeAuthPassword('mysecretpassword')
            >>> node = driver.create_node("test_node", auth=auth, ...)

        If you don't provide the ``auth`` argument libcloud will assign
        a password::

            >>> driver = AzureNodeDriver(...)
            >>> node = driver.create_node("test_node", ...)
            >>> password = node.extra["properties"] \
                           ["osProfile"]["adminPassword"]

        :param name:   String with a name for this new node (required)
        :type name:   ``str``

        :param size:   The size of resources allocated to this node.
                            (required)
        :type size:   :class:`.NodeSize`

        :param image:  OS Image to boot on node. (required)
        :type image:  :class:`.AzureImage`

        :param location: Which data center to create a node in.
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param auth:   Initial authentication information for the node
                            (optional)
        :type auth:   :class:`.NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :param ex_resource_group:  The resource group in which to create the
        node
        :type ex_resource_group: ``str``

        :param ex_storage_account:  The storage account id in which to store
        the node's disk image.
        Note: when booting from a user image (AzureVhdImage)
        the source image and the node image must use the same storage account.
        :type ex_storage_account: ``str``

        :param ex_blob_container:  The name of the blob container on the
        storage account in which to store the node's disk image (optional,
        default "vhds")
        :type ex_blob_container: ``str``

        :param ex_user_name:  User name for the initial admin user
        (optional, default "azureuser")
        :type ex_user_name: ``str``

        :param ex_network: The virtual network the node will be attached to.
        Must provide either `ex_network` (to create a default NIC for the
        node on the given network) or `ex_nic` (to supply the NIC explicitly).
        :type ex_network: ``str``

        :param ex_subnet: If ex_network is provided, the subnet of the
        virtual network the node will be attached to.  Optional, default
        is the "default" subnet.
        :type ex_subnet: ``str``

        :param ex_nic: A virtual NIC to attach to this Node, from
        `ex_create_network_interface` or `ex_get_nic`.
        Must provide either `ex_nic` (to supply the NIC explicitly) or
        ex_network (to create a default NIC for the node on the
        given network).
        :type ex_nic: :class:`AzureNic`

        :param ex_tags: Optional tags to associate with this node.
        :type ex_tags: ``dict``

        :param ex_customdata: Custom data that will
            be placed in the file /var/lib/waagent/CustomData
            https://azure.microsoft.com/en-us/documentation/ \
            articles/virtual-machines-how-to-inject-custom-data/
        :type ex_customdata: ``str``

        :return: The newly created node.
        :rtype: :class:`.Node`

        """

        if location is None:
            location = self.default_location
        if ex_nic is None:
            if ex_network is None:
                raise ValueError("Must provide either ex_network or ex_nic")
            if ex_subnet is None:
                ex_subnet = "default"

            subnet_id = "/subscriptions/%s/resourceGroups/%s/providers" \
                        "/Microsoft.Network/virtualnetworks/%s/subnets/%s" % \
                        (self.subscription_id, ex_resource_group,
                         ex_network, ex_subnet)
            subnet = AzureSubnet(subnet_id, ex_subnet, {})
            ex_nic = self.ex_create_network_interface(name + "-nic",
                                                      subnet,
                                                      ex_resource_group,
                                                      location)

        auth = self._get_and_check_auth(auth)

        target = "/subscriptions/%s/resourceGroups/%s/providers" \
                 "/Microsoft.Compute/virtualMachines/%s" % \
                 (self.subscription_id, ex_resource_group, name)

        instance_vhd = "https://%s.blob.core.windows.net" \
                       "/%s/%s-os.vhd" \
                       % (ex_storage_account,
                          ex_blob_container,
                          name
                          )

        if isinstance(image, AzureVhdImage):
            storageProfile = {
                "osDisk": {
                    "name": "virtualmachine-osDisk",
                    "osType": "linux",
                    "caching": "ReadWrite",
                    "createOption": "FromImage",
                    "image": {
                        "uri": image.id
                    },
                    "vhd": {
                        "uri": instance_vhd
                    }
                }
            }
        elif isinstance(image, AzureImage):
            storageProfile = {
                "imageReference": {
                    "publisher": image.publisher,
                    "offer": image.offer,
                    "sku": image.sku,
                    "version": image.version
                },
                "osDisk": {
                    "name": "virtualmachine-osDisk",
                    "vhd": {
                        "uri": instance_vhd
                    },
                    "caching": "ReadWrite",
                    "createOption": "FromImage"
                }
            }
        else:
            raise LibcloudError(
                "Unknown image type %s,"
                "expected one of AzureImage, AzureVhdImage",
                type(image))

        data = {
            "id": target,
            "name": name,
            "type": "Microsoft.Compute/virtualMachines",
            "location": location.id,
            "tags": ex_tags,
            "properties": {
                "hardwareProfile": {
                    "vmSize": size.id
                },
                "storageProfile": storageProfile,
                "osProfile": {
                    "computerName": name
                },
                "networkProfile": {
                    "networkInterfaces": [
                        {
                            "id": ex_nic.id
                        }
                    ]
                }
            }
        }

        if ex_customdata:
            data["properties"]["osProfile"]["customData"] = \
                base64.b64encode(ex_customdata)

        data["properties"]["osProfile"]["adminUsername"] = ex_user_name

        if isinstance(auth, NodeAuthSSHKey):
            data["properties"]["osProfile"]["adminPassword"] = \
                binascii.hexlify(os.urandom(20)).decode("utf-8")
            data["properties"]["osProfile"]["linuxConfiguration"] = {
                "disablePasswordAuthentication": "true",
                "ssh": {
                    "publicKeys": [
                        {
                            "path": '/home/%s/.ssh/authorized_keys' % (
                                ex_user_name),
                            "keyData": auth.pubkey
                        }
                    ]
                }
            }
        elif isinstance(auth, NodeAuthPassword):
            data["properties"]["osProfile"]["linuxConfiguration"] = {
                "disablePasswordAuthentication": "false"
            }
            data["properties"]["osProfile"]["adminPassword"] = auth.password
        else:
            raise ValueError(
                "Must provide NodeAuthSSHKey or NodeAuthPassword in auth")

        r = self.connection.request(target,
                                    params={"api-version": "2015-06-15"},
                                    data=data,
                                    method="PUT")

        while r.object is None:
            time.sleep(1)

        node = self._to_node(r.object)
        node.size = size
        node.image = image
        return node
