# Copyright 2018 Twitter, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

""" This module contains helper functions associated with packing plans. """

import logging
from collections import defaultdict, namedtuple
from jsonschema import validate
from typing import Dict

LOG: logging.Logger = logging.getLogger(__name__)

InstanceInfo = namedtuple('InstanceInfo', ['parallelism', 'CPU', 'RAM', 'Disk', 'tasks'])

def validate_packing_plan(json_packing_plan) -> bool:
    schema = {
        "definitions": {
            "resource": {
                "type": "object",
                "properties": {
                    "cpu": {"type": "number"},
                    "ram": {"type": "integer"},
                    "disk": {"type": "integer"}
                },
                "required": ["cpu", "ram", "disk"]
            },
            "instance": {
                "properties": {
                    "instance_resources": {
                        "maxProperties": 1,
                        "minProperties": 1,
                        "$ref": "#/definitions/resource"},
                    "component_name": {"type": "string"},
                    "task_id": {"type": "integer"}
                },
                "required": ["instance_resources", "component_name", "task_id"]
            },
            "container_plan": {
                "type": "object",
                "properties": {
                    "scheduled_resources": {
                        "maxProperties": 1,
                        "minProperties": 0,
                        "$ref": "#/definitions/resource"
                    },
                    "instances": {"type": "array",
                                  "items": {
                                      "$ref": "#/definitions/instance"}
                                  },
                    "required_resources": {
                        "maxProperties": 1,
                        "minProperties": 1,
                        "$ref": "#/definitions/resource"}
                },
                "required": ["required_resources", "instances"]
            }
        },
        "type": "object",
        "properties": {
            "container_plans": {
                "type": "array",
                "items": {

                    "$ref": "#/definitions/container_plan"
                }
            }
        },
        "required": ["container_plans"]
    }

    validate(json_packing_plan, schema)


def summarize_packing_plans(packing_plan) -> Dict[int, tuple]:
    # We assume that the resources of all instances are the same
    # TODO: How do we factors in different container sizes + padding?
    instance_count = {}
    instance_cpu = {}
    instance_ram = {}
    instance_disk = {}
    instance_task_ids = defaultdict(list)
    result = {}

    container_plans = packing_plan["container_plans"]
    for container_plan in container_plans:
        for instance in container_plan["instances"]:
            comp_name = instance["component_name"]
            count = instance_count.get(comp_name, 0) + 1
            instance_count[comp_name] = count
            if comp_name not in instance_cpu:
                instance_cpu[comp_name] = instance["instance_resources"]["cpu"]
                instance_ram[comp_name] = instance["instance_resources"]["ram"]
                instance_disk[comp_name] = instance["instance_resources"]["disk"]
                instance_task_ids[comp_name] = [instance["task_id"]]
            else:
                instance_task_ids[comp_name].append(instance["task_id"])

    for comp_name in instance_count:
        result[comp_name] = InstanceInfo(instance_count[comp_name], instance_cpu[comp_name],
                                         instance_ram[comp_name], instance_disk[comp_name],
                                         instance_task_ids[comp_name])

    return result
