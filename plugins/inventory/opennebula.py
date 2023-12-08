import collections
import logging

from enum import Enum

from ansible.errors import AnsibleError, AnsibleOptionsError, AnsibleRuntimeError
from ansible.plugins.inventory import BaseInventoryPlugin, Constructable, Cacheable
from ansible.module_utils.common.text.converters import to_text
from ansible.utils.display import Display

ANSIBLE_METADATA = {
    'metadata_version': '',
    'status': [],
    'supported_by': ''
}

DOCUMENTATION = '''
---
module: opennebula
plugin_type: inventory
short_description: OpenNebula inventory plugin
author: Kaloyan Kotlarski
description:
  - Retrieves inventory hosts from OpenNebula deployments
options:
  one_url:
    type: string
    description: OpenNebula RPC endpoint URL
    required: True
    default: http://localhost:2633/RPC2
    env:
      - name: ONE_URL
  one_username:
    type: string
    description: OpenNebula username to authenticate with
    required: True
    default: oneadmin
    env:
      - name: ONE_USERNAME
  one_password:
    type: string
    description: OpenNebula password to authenticate with
    required: True
    env:
      - name: ONE_PASSWORD
  one_hostname_preference:
    type: string
    description: Controls whether the VM will be inserted with a FQDN or with its name
    required: False
    choices:
      - fqdn
      - name
    default: fqdn
extends_documentation_fragment:
  - inventory_cache
  - constructed   
'''

display = Display()
logger = logging.getLogger('opennebula')

HAS_PYONE_MODULE = False

try:
    import pyone

    HAS_PYONE_MODULE = True
except ImportError:
    HAS_PYONE_MODULE = False


class State(Enum):
    init = 0
    pending = 1
    hold = 2
    active = 3
    stopped = 4
    suspended = 5
    done = 6
    poweroff = 8
    undeployed = 9
    cloning = 10
    cloning_failure = 11


def one_dict_to_lowercase(one_dict):
    result = {}

    for key in one_dict.keys():
        if key == "#text":
            continue

        value = one_dict[key]

        if type(value) == str and len(one_dict[key]):
            result[key.lower()] = to_text(one_dict[key])
        elif type(value) == dict:
            one_dict_to_lowercase(value)

    return result


def get_domain_name_for_network(server, network_id):
    vm_virtual_network = server.vn.info(int(network_id))

    if "DOMAIN" in vm_virtual_network.TEMPLATE:
        return vm_virtual_network.TEMPLATE["DOMAIN"][:-1]
    else:
        return None


class InventoryModule(BaseInventoryPlugin, Constructable, Cacheable):
    """Host Inventory provider for ansible using OpenNebula"""

    NAME = "netcho.opennebula.opennebula"

    def _get_vmpool(self):
        try:
            return self.server.vmpool.infoextended(-2, -1, -1, -1)
        except pyone.OneException as e:
            raise AnsibleRuntimeError(e.message)

    def _get_dict_for_vm(self, vm):
        vm_state = State(vm.get_STATE())
        vm_lcm_state = pyone.LCM_STATE(vm.get_LCM_STATE())

        vm_dict = {
            "id": vm.get_ID(),
            "name": vm.get_NAME(),
            "state": vm_state.name,
            "lcm_state": str(vm_lcm_state.name).lower(),
            "deploy_id": vm.get_DEPLOY_ID(),
            "start_timestamp": vm.get_STIME(),
            "nic": [],
            "network_id_domain_map": {}
        }

        vm_template = vm.get_TEMPLATE()
        if vm_template is not None:
            if "TEMPLATE_ID" in vm_template:
                vm_dict["template_id"] = int(vm_template["TEMPLATE_ID"])
                try:
                    template_info = self.server.template.info(vm_dict["template_id"])
                    vm_dict["template"] = template_info.get_NAME()
                except pyone.OneNoExistsException:
                    display.vvv(f"VM {vm.get_NAME()} template ID {vm_template['TEMPLATE_ID']} doesn't not exist, not retrieving it")
                    pass
                except pyone.OneException as e:
                    raise AnsibleRuntimeError(str(e))

            if "NIC" in vm_template:
                vm_nics = []

                if isinstance(vm_template["NIC"], dict):
                    vm_nics.append(vm_template["NIC"])
                elif isinstance(vm.TEMPLATE["NIC"], list):
                    vm_nics = vm_template["NIC"]

                for nic in vm_nics:
                    vm_dict["nic"].append(one_dict_to_lowercase(nic))

                    network_domain_name = get_domain_name_for_network(self.server, nic["NETWORK_ID"])

                    if network_domain_name is not None:
                        vm_dict["network_id_domain_map"][nic["NETWORK_ID"]] = network_domain_name

        if hasattr(vm, "USER_TEMPLATE"):
            vm_dict["user_attributes"] = one_dict_to_lowercase(vm.USER_TEMPLATE)

        return vm_dict

    def _query(self):
        return [self._get_dict_for_vm(vm) for vm in self._get_vmpool().VM]

    def _get_hostname(self, vm):
        hostname_preference = self.get_option("one_hostname_preference")
        if not hostname_preference:
            raise AnsibleOptionsError(
                f"Invalid value for option one_hostname_preference: {hostname_preference}")

        if hostname_preference == "fqdn":
            domain = get_domain_name_for_network(self.server, vm["nic"][0]["network_id"])

            if domain is not None:
                return to_text(vm["name"] + "." + domain)
            else:
                display.vvvv(f"VM {vm['name']} first NIC doesn't have a domain configured, using VM name")
                return vm["name"]
        elif hostname_preference == "name":
            return vm["name"]

    def _populate_from_source(self, source_data):
        for host in source_data:
            if not len(host["nic"]):
                display.v(
                   f"VM {vm['name']} doesn't have any NICs attached to it, skipping it.")
                continue

            hostname = self._get_hostname(host)

            self.inventory.add_host(hostname)

            for fact, value in host.items():
                self.inventory.set_variable(hostname, fact, value)

            strict = self.get_option('strict')
            self._set_composite_vars(self.get_option('compose'), host, hostname, strict=strict)
            self._add_host_to_composed_groups(self.get_option('groups'), host, hostname, strict=strict)
            self._add_host_to_keyed_groups(self.get_option('keyed_groups'), host, hostname, strict=strict)

    def verify_file(self, path):
        if super(InventoryModule, self).verify_file(path):
            if path.endswith(("one.yaml", "one.yml")):
                return True
        return False

    def parse(self, inventory, loader, path, cache=True):
        if not HAS_PYONE_MODULE:
            raise AnsibleError("OpenNebula inventory plugin requires pyone module to be installed")

        super(InventoryModule, self).parse(inventory, loader, path)

        config = self._read_config_data(path)

        self.server = pyone.OneServer(self.get_option("one_url"),
                                      self.get_option("one_username") + ":" + self.get_option("one_password"))

        cache_key = self.get_cache_key(path)
        source_data = None

        user_cache_setting = self.get_option('cache')
        attempt_to_read_cache = user_cache_setting and cache
        cache_needs_update = user_cache_setting and not cache

        if attempt_to_read_cache:
            try:
                source_data = self._cache[cache_key]
            except KeyError:
                cache_needs_update = True

        if source_data is None:
            source_data = self._query()

        if cache_needs_update:
            self._cache[cache_key] = source_data

        self._populate_from_source(source_data)
