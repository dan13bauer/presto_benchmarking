#!/usr/bin/env python3
"""
Simple Query Plan Visualizer - Clean, readable query execution plan viewer.

Creates a simple, text-based HTML visualization focusing on readability
instead of interactive graphics. Shows stages, operators, and data flow
in an easy-to-understand format.

Usage:
    python query_plan_simple.py <path_to_query_json> [--output <output_html>]
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, List


def format_data_size(bytes_val) -> str:
    """Format bytes to human-readable size."""
    if isinstance(bytes_val, str):
        return bytes_val
    if bytes_val is None or bytes_val == 0:
        return "0B"
    try:
        size = float(bytes_val)
    except (ValueError, TypeError):
        return str(bytes_val)

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for unit in units:
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f}PB"


def format_rows(rows) -> str:
    """Format row count."""
    if rows is None or rows == 0:
        return "0"
    if isinstance(rows, str):
        return rows
    rows = int(rows)
    if rows < 1000:
        return str(rows)
    if rows < 1_000_000:
        return f"{rows / 1000:.2f}K"
    if rows < 1_000_000_000:
        return f"{rows / 1_000_000:.2f}M"
    return f"{rows / 1_000_000_000:.2f}B"


def parse_time_value(time_str: str) -> float:
    """Parse time string (e.g., '49.06us', '80.54ms', '6.24m') to nanoseconds."""
    if not isinstance(time_str, str) or time_str == "0.00ns":
        return 0.0

    time_str = time_str.strip()

    # Extract number and unit
    import re
    match = re.match(r'([0-9.]+)\s*([a-zA-Z]+)', time_str)
    if not match:
        return 0.0

    value = float(match.group(1))
    unit = match.group(2).lower()

    # Convert to nanoseconds
    conversions = {
        'ns': 1,
        'us': 1_000,
        'ms': 1_000_000,
        's': 1_000_000_000,
        'm': 60 * 1_000_000_000,  # minutes
        'h': 3600 * 1_000_000_000,  # hours
    }

    multiplier = conversions.get(unit, 1)
    return value * multiplier


def format_time(nanoseconds: float) -> str:
    """Format nanoseconds to human-readable time."""
    if nanoseconds == 0:
        return "0.00ns"

    units = [
        (1_000_000_000 * 3600, 'h'),
        (1_000_000_000 * 60, 'm'),
        (1_000_000_000, 's'),
        (1_000_000, 'ms'),
        (1_000, 'us'),
        (1, 'ns'),
    ]

    for divisor, unit in units:
        if nanoseconds >= divisor:
            return f"{nanoseconds / divisor:.2f}{unit}"

    return f"{nanoseconds:.2f}ns"


def calculate_total_walltime(operator: Dict[str, Any]) -> str:
    """Calculate total walltime for an operator by summing all walltime components."""
    walltime_fields = ['addInputWall', 'getOutputWall', 'finishWall', 'blockedWall', 'isBlockedWall']

    total_ns = 0.0
    for field in walltime_fields:
        time_str = operator.get(field, "0.00ns")
        total_ns += parse_time_value(time_str)

    return format_time(total_ns)


def build_operator_connections(query_info: Dict[str, Any]) -> Dict[tuple, tuple]:
    """
    Build bi-directional connections between operators using PURE TREE-BASED LOGIC.

    Core principle: The logical plan tree structure encodes ALL data flow relationships.

    - An operator outputs data if its plan node has children implemented in other pipelines
    - An operator receives inter-stage data if it's referenced in a node's remoteSources
    - Destinations are determined by following the logical plan tree, not operator types

    This eliminates all hardcoded operator type lists by deriving behavior from tree structure.

    Connection key: (stage_id, pipeline_id, operator_id) -> (dest_stage_id, dest_pipeline_id, dest_operator_id, direction)
    Direction: 'forward' (sender to receiver) or 'reverse' (receiver to sender)
    """
    connections = {}

    def add_connection(src_key, dst_key):
        """Add both forward and reverse connections (with deduplication)"""
        if src_key not in connections:
            connections[src_key] = []

        # Check if this forward connection already exists
        if not any(d[0] == dst_key and d[1] == 'forward' for d in connections[src_key]):
            connections[src_key].append((dst_key, 'forward'))

        if dst_key not in connections:
            connections[dst_key] = []

        # Check if this reverse connection already exists
        if not any(d[0] == src_key and d[1] == 'reverse' for d in connections[dst_key]):
            connections[dst_key].append((src_key, 'reverse'))

    operator_summaries = query_info.get("queryStats", {}).get("operatorSummaries", [])

    # Stage 1: Index operators
    stage_pipeline_ops = {}  # (stage, pipeline) -> [operators]
    stage_ops = {}  # stage -> [operators]
    node_to_operators = {}  # (stage, plan_node_id) -> [operators]
    operator_summaries_by_key = {}  # (stage, pipeline, op_id) -> operator

    for op in operator_summaries:
        stage_id = op.get("stageId")
        pipeline_id = op.get("pipelineId", 0)
        op_id = op.get("operatorId")
        plan_node_id = op.get("planNodeId")

        # Index by (stage, pipeline, op_id) for quick lookup
        op_key = (stage_id, pipeline_id, op_id)
        operator_summaries_by_key[op_key] = op

        # Index by (stage, pipeline)
        key = (stage_id, pipeline_id)
        if key not in stage_pipeline_ops:
            stage_pipeline_ops[key] = []
        stage_pipeline_ops[key].append(op)

        # Index by stage
        if stage_id not in stage_ops:
            stage_ops[stage_id] = []
        stage_ops[stage_id].append(op)

        # Index by (stage, plan_node_id)
        # Normalize plan_node_id to string for consistent lookups
        if plan_node_id:
            plan_node_id_str = str(plan_node_id)
            node_key = (stage_id, plan_node_id_str)
            if node_key not in node_to_operators:
                node_to_operators[node_key] = []
            node_to_operators[node_key].append(op)

    # Stage 2: Extract and analyze logical plan structure for each stage
    stage_plans = {}  # stage_id -> plan tree
    stage_tree_structure = {}  # stage_id -> node_id -> {children: [...], remoteSources: [...]}

    def extract_stage_plans(stage, target_stage_id=None):
        """Recursively extract stage plans from the query structure."""
        stage_id = stage.get("plan", {}).get("id")
        if stage_id:
            try:
                stage_id = int(stage_id)
            except (ValueError, TypeError):
                pass

            # Only extract if we haven't already or if target matches
            if target_stage_id is None or stage_id == target_stage_id:
                plan_data = stage.get("plan", {}).get("jsonRepresentation", "{}")
                try:
                    plan_tree = json.loads(plan_data)
                    stage_plans[stage_id] = plan_tree

                    # Build tree structure for this stage
                    tree = {}

                    def build_tree_map(node):
                        """Build map of node structure from plan tree."""
                        node_id = node.get("id")
                        if node_id:
                            tree[node_id] = {
                                "name": node.get("name", ""),
                                "children": [c.get("id") for c in node.get("children", [])],
                                "remoteSources": [int(x) for x in node.get("remoteSources", [])]
                            }

                        for child in node.get("children", []):
                            build_tree_map(child)

                    build_tree_map(plan_tree)
                    stage_tree_structure[stage_id] = tree

                except (json.JSONDecodeError, TypeError):
                    pass

        for substage in stage.get("subStages", []):
            extract_stage_plans(substage, target_stage_id)

    if "outputStage" in query_info:
        extract_stage_plans(query_info["outputStage"])

    # Stage 3: Determine output behavior based on tree structure
    # This replaces the hardcoded operator type lists
    def get_intra_stage_children(stage_id, node_id):
        """Get children of a node that are in different pipelines in the SAME stage."""
        if stage_id not in stage_tree_structure:
            return []

        tree = stage_tree_structure[stage_id]
        if node_id not in tree:
            return []

        children = tree[node_id].get("children", [])
        result = []

        # Get pipelines implementing this node
        # Normalize node_id to string for lookup
        node_id_str = str(node_id)
        node_key = (stage_id, node_id_str)
        producer_pipelines = set(op.get("pipelineId", 0)
                                for op in node_to_operators.get(node_key, []))

        # Check which children are in this stage in different pipelines
        for child_id in children:
            child_id_str = str(child_id)

            # Skip inter-stage children (RemoteSource nodes with remoteSources)
            # These should not be routed to intra-stage
            child_tree_info = tree.get(child_id_str, {})
            if child_tree_info.get("remoteSources", []):
                # This is an inter-stage receiver, skip it
                continue

            child_key = (stage_id, child_id_str)
            child_ops = node_to_operators.get(child_key, [])

            if child_ops:  # Child is in this stage
                child_pipelines = set(op.get("pipelineId", 0) for op in child_ops)

                # If child is in different pipeline(s), it's an intra-stage connection target
                for child_p in child_pipelines:
                    for prod_p in producer_pipelines:
                        if child_p != prod_p:
                            result.append(child_id)
                            break

        return result

    # Stage 4: Process each pipeline and determine its connections
    for (stage_id, pipeline_id), ops_in_pipeline in sorted(stage_pipeline_ops.items()):
        # Find highest operator in this pipeline (by operatorId) that implements a tree node
        # Skip terminal operators that don't implement tree nodes
        sorted_ops = sorted(ops_in_pipeline, key=lambda x: x.get("operatorId", 0))
        if not sorted_ops:
            continue

        # Find highest operator that implements a tree node
        # If highest operator doesn't have valid planNodeId, find next highest that does
        highest_op = None
        highest_op_with_node = None

        for op in reversed(sorted_ops):
            op_type = op.get("operatorType", "")
            op_node_id = op.get("planNodeId")

            # Track the very highest operator
            if highest_op is None:
                highest_op = op

            # Track highest operator with valid planNodeId (only numeric IDs that are in the tree)
            if highest_op_with_node is None and op_node_id and op_node_id != "N/A":
                # Only use numeric node IDs (not IDs like "1653-to-velox")
                try:
                    int(op_node_id)
                    # Node ID is numeric, this is a valid fallback
                    highest_op_with_node = op
                except (ValueError, TypeError):
                    # Non-numeric node ID, skip it
                    pass

        if not highest_op:
            continue

        # Determine which operator to use for routing
        # If highest operator is a terminal/no-output operator, use the highest operator with a valid node ID
        highest_op_type = highest_op.get("operatorType", "")
        no_output_operators = [
            "HashJoinBuild", "CudfHashJoinBuild",
            "HashJoinProbe", "CudfHashJoinProbe",
            "TopNRowNumber", "CudfTopNRowNumber",
        ]

        # Determine the operator to use for routing (needs valid node ID)
        routing_op = highest_op
        if highest_op_type in no_output_operators or not highest_op.get("planNodeId") or highest_op.get("planNodeId") == "N/A":
            # Use highest operator with valid node ID for determining routing
            if highest_op_with_node:
                routing_op = highest_op_with_node
            else:
                # No operator in this pipeline has a valid node ID
                continue

        # connection_key is ALWAYS the actual highest operator (for the connection source)
        connection_key = (stage_id, pipeline_id, highest_op.get("operatorId"))

        # routing_op provides the node ID for determining WHERE to send data
        routing_op_type = routing_op.get("operatorType", "")
        routing_op_node_id = routing_op.get("planNodeId")
        highest_op_type = highest_op.get("operatorType", "")

        # Skip if the selected operator is a no-output operator
        # (these should never create connections)
        if highest_op_type in no_output_operators:
            continue

        # PartitionedOutput operators handle inter-stage connections only in Stage 5
        # Skip them here to avoid double-connecting
        if "PartitionedOutput" in highest_op_type or "cudfPartitionedOutput" in highest_op_type:
            continue

        # Skip if no valid node ID or stage not in tree
        if not routing_op_node_id or routing_op_node_id == "N/A" or stage_id not in stage_tree_structure:
            continue

        tree = stage_tree_structure[stage_id]
        if routing_op_node_id not in tree:
            continue

        node_info = tree[routing_op_node_id]

        # Check if this node has inter-stage children (remoteSources)
        remote_sources = node_info.get("remoteSources", [])

        if remote_sources:
            # Inter-stage output: Connect to receiving stages
            for receiving_stage_id in remote_sources:
                # Find operator in receiving stage that implements a child node
                for child_id in node_info.get("children", []):
                    child_key = (receiving_stage_id, child_id)
                    child_ops = node_to_operators.get(child_key, [])

                    for dest_op in child_ops:
                        dest_key = (
                            receiving_stage_id,
                            dest_op.get("pipelineId", 0),
                            dest_op.get("operatorId")
                        )
                        add_connection(connection_key, dest_key)
        else:
            # Skip intra-stage connections - they are too complex to encode
            pass

    # Stage 5: Connect PartitionedOutput operators to inter-stage RECEIVERS
    # A PartitionedOutput in stage X sends to stages that have RemoteSource nodes with remoteSources=['X']
    # Each PartitionedOutput sends to exactly ONE receiving operator per receiving stage

    for (stage_id, pipeline_id), ops_in_pipeline in sorted(stage_pipeline_ops.items()):
        for op in ops_in_pipeline:
            op_type = op.get("operatorType", "")
            if not ("PartitionedOutput" in op_type or "cudfPartitionedOutput" in op_type):
                continue

            # This is a PartitionedOutput operator in stage_id
            source_key = (stage_id, pipeline_id, op.get("operatorId"))

            # Find all stages that have receiver nodes with remoteSources pointing to this stage
            # by checking all stages' plans for nodes with remoteSources=[stage_id]
            for recv_stage_id, recv_tree_structure in stage_tree_structure.items():
                if recv_stage_id == stage_id:
                    continue  # Skip same stage

                # Look for nodes in recv_stage with remoteSources=[stage_id]
                for node_id, node_info in recv_tree_structure.items():
                    remote_srcs = node_info.get("remoteSources", [])

                    # Check if this node receives from our source stage
                    if stage_id in remote_srcs:
                        # Found a receiver in recv_stage_id that receives from stage_id
                        # Get the operators implementing this receiving node
                        node_id_str = str(node_id)
                        recv_node_key = (recv_stage_id, node_id_str)
                        recv_ops = node_to_operators.get(recv_node_key, [])

                        if recv_ops:
                            # Connect to Op 0 of each pipeline implementing this receiving node
                            for recv_op in recv_ops:
                                recv_pipeline = recv_op.get("pipelineId", 0)
                                recv_key = (recv_stage_id, recv_pipeline)

                                if recv_key in stage_pipeline_ops:
                                    recv_ops_list = stage_pipeline_ops[recv_key]
                                    op_0 = next((o for o in recv_ops_list if o.get("operatorId") == 0), None)

                                    if op_0:
                                        dest_key = (
                                            recv_stage_id,
                                            op_0.get("pipelineId", 0),
                                            op_0.get("operatorId")
                                        )
                                        add_connection(source_key, dest_key)

                            # Only connect to first matching receiver
                            break

    return connections


def extract_stages(query_info: Dict[str, Any]) -> tuple:
    """Extract runtime operators from query execution stats."""
    stages_list = []

    # Get runtime operators from operatorSummaries
    operator_summaries = query_info.get("queryStats", {}).get("operatorSummaries", [])

    # Build connections between operators
    connections = build_operator_connections(query_info)

    # Group operators by stage
    stage_operators = {}
    for op in operator_summaries:
        stage_id = op.get("stageId")
        if stage_id not in stage_operators:
            stage_operators[stage_id] = []
        stage_operators[stage_id].append(op)

    # Get stage information from the plan tree for stats
    stage_stats = {}
    if "outputStage" in query_info:
        def collect_stage_stats(stage: Dict[str, Any]) -> None:
            stage_id = stage.get("plan", {}).get("id", "unknown")
            try:
                stage_id = int(stage_id)
            except (ValueError, TypeError):
                pass

            stats = stage.get("latestAttemptExecutionInfo", {}).get("stats", {})
            stage_stats[stage_id] = {
                "state": stage.get("latestAttemptExecutionInfo", {}).get("state", "UNKNOWN"),
                "stats": stats,
            }

            for substage in stage.get("subStages", []):
                collect_stage_stats(substage)

        collect_stage_stats(query_info["outputStage"])

    # Create stage entries with runtime operators
    for stage_id in sorted(stage_operators.keys()):
        ops = stage_operators[stage_id]

        # IMPORTANT: Sort operators by operatorId within each stage
        # (JSON may have them in arbitrary order, but HTML display depends on proper ordering)
        ops = sorted(ops, key=lambda x: x.get("operatorId", 0))

        # Convert operators to simplified format
        runtime_ops = []
        for op in ops:
            runtime_ops.append({
                "name": op.get("operatorType", "Unknown"),
                "operator_id": op.get("operatorId"),
                "pipeline_id": op.get("pipelineId"),
                "plan_node_id": op.get("planNodeId", "N/A"),
                "input_data_size": op.get("inputDataSizeInBytes", 0),
                "output_data_size": op.get("outputDataSizeInBytes", 0),
                "cpu_time": op.get("totalCpuTime", "N/A"),
                "raw_input_data_size": op.get("rawInputDataSizeInBytes", 0),
                "input_positions": op.get("inputPositions", 0),
                "output_positions": op.get("outputPositions", 0),
                "wall_time": calculate_total_walltime(op),
            })

        stage_info = stage_stats.get(stage_id, {})

        stages_list.append({
            "stageId": stage_id,
            "planId": str(stage_id),
            "state": stage_info.get("state", "UNKNOWN"),
            "distribution": "",
            "operators": runtime_ops,
            "stats": stage_info.get("stats", {}),
            "depth": 0,
        })

    # Sort by stage ID
    try:
        stages_list.sort(key=lambda s: int(s["planId"]))
    except (ValueError, TypeError):
        pass

    return stages_list, connections


def is_blocking_operator(name: str) -> bool:
    """Check if an operator is blocking (separates pipelines)."""
    blocking_types = {
        "Exchange", "LocalExchange", "RemoteSource", "RemoteExchange",
        "Join", "InnerJoin", "LeftJoin", "RightJoin", "FullOuterJoin",
        "Aggregate", "GroupBy", "Window", "TopN", "Sort",
        "TableScan", "TableWrite",
    }
    return any(op in name for op in blocking_types)


def extract_operators(node: Dict[str, Any], operators: List[Dict] = None, pipeline_id: int = 0) -> tuple:
    """Recursively extract operators from plan node with pipeline IDs."""
    if operators is None:
        operators = []

    current_pipeline = pipeline_id

    operator = {
        "id": node.get("id", ""),
        "name": node.get("name", ""),
        "identifier": node.get("identifier", ""),
        "details": node.get("details", ""),
        "sources": len(node.get("children", [])),
        "remoteSources": node.get("remoteSources", []),
        "pipeline_id": current_pipeline,
    }
    operators.append(operator)

    # If this is a blocking operator, increment pipeline for children
    next_pipeline = current_pipeline + 1 if is_blocking_operator(node.get("name", "")) else current_pipeline

    # Process children (in reverse order to maintain top-level flow)
    for child in reversed(node.get("children", [])):
        extract_operators(child, operators, next_pipeline)

    return operators


def generate_html(query_plan_json: Dict[str, Any]) -> str:
    """Generate simple HTML visualization."""

    stages, connections = extract_stages(query_plan_json)
    query_info = query_plan_json.get("queryStats", {})
    query_text = query_plan_json.get("query", "N/A")
    query_id = query_plan_json.get("queryId", "N/A")
    state = query_plan_json.get("state", "UNKNOWN")

    # Color based on state
    state_colors = {
        "FINISHED": "#2ecc71",
        "RUNNING": "#3498db",
        "FAILED": "#e74c3c",
        "CANCELLED": "#95a5a6",
        "SCHEDULED": "#f39c12",
        "QUEUED": "#9b59b6",
    }
    state_color = state_colors.get(state, "#95a5a6")

    # Build merged operator summary with wall time and stages
    operator_type_stats = {}  # operator_type -> {wall_time_ns: ..., stages: [...], count: ...}

    for stage in stages:
        for op in stage["operators"]:
            op_type = op.get("name", "Unknown")
            wall_time_str = op.get("wall_time", "0.00ns")
            wall_time_ns = parse_time_value(wall_time_str)
            stage_id = stage["stageId"]

            if op_type not in operator_type_stats:
                operator_type_stats[op_type] = {
                    "wall_time_ns": 0,
                    "stages": set(),
                    "count": 0
                }

            operator_type_stats[op_type]["wall_time_ns"] += wall_time_ns
            operator_type_stats[op_type]["stages"].add(stage_id)
            operator_type_stats[op_type]["count"] += 1

    # Sort by total wall time descending
    sorted_operator_types = sorted(
        operator_type_stats.items(),
        key=lambda x: x[1]["wall_time_ns"],
        reverse=True
    )

    # Build merged operator summary HTML
    operator_summary_html = '<div class="operator-summary">\n    <div class="operator-summary-title">Operators by Type & Wall Time</div>\n    <table class="operator-summary-table">\n        <thead>\n            <tr>\n                <th>Operator Type</th>\n                <th style="text-align: center;">Count</th>\n                <th style="text-align: right;">Total Wall Time</th>\n                <th style="text-align: left;">Stages</th>\n            </tr>\n        </thead>\n        <tbody>\n'
    for op_type, stats in sorted_operator_types:
        wall_time_str = format_time(stats["wall_time_ns"])
        stages_list = sorted(stats["stages"])
        stages_str = ", ".join(str(s) for s in stages_list)
        count = stats["count"]
        operator_summary_html += f'            <tr><td>{op_type}</td><td style="text-align: center;">{count}</td><td style="text-align: right; font-family: monospace;">{wall_time_str}</td><td style="font-family: monospace;">{stages_str}</td></tr>\n'
    operator_summary_html += '        </tbody>\n    </table>\n</div>\n'

    # Build stage navigation
    stage_nav_html = '<div class="stage-nav">\n        <div class="stage-nav-title">Stages</div>\n        <div class="stage-nav-list">\n'
    for stage in stages:
        stage_nav_html += f'            <a href="#stage-{stage["stageId"]}" class="stage-nav-link">Stage {stage["planId"]}</a>\n'
    stage_nav_html += '        </div>\n    </div>\n'

    # Build stages HTML
    stages_html = ""
    for idx, stage in enumerate(stages):
        stats = stage["stats"]

        # Stage header
        stages_html += f"""
        <div class="stage" id="stage-{stage['stageId']}">
            <div class="stage-header" style="background-color: {state_color}">
                <div class="stage-title">
                    <strong>Stage {stage['planId']}</strong>
                    <span class="state-badge">{stage['state']}</span>
                </div>
                <div class="stage-id">ID: {stage['stageId']}</div>
            </div>

            <div class="stage-content">
                <div class="stage-stats">
                    <div class="stat-row">
                        <span class="stat-label">CPU Time:</span>
                        <span class="stat-value">{stats.get('totalCpuTime', 'N/A')}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Memory:</span>
                        <span class="stat-value">{format_data_size(stats.get('userMemoryReservationInBytes', 0))}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Splits (Q/R/F):</span>
                        <span class="stat-value">{stats.get('queuedDrivers', 0)}/{stats.get('runningDrivers', 0)}/{stats.get('completedDrivers', 0)}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Input:</span>
                        <span class="stat-value">{format_data_size(stats.get('rawInputDataSizeInBytes', 0))} / {format_rows(stats.get('rawInputPositions', 0))} rows</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Output:</span>
                        <span class="stat-value">{format_data_size(stats.get('outputDataSizeInBytes', 0))} / {format_rows(stats.get('outputPositions', 0))} rows</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Buffered:</span>
                        <span class="stat-value">{format_data_size(stats.get('bufferedDataSizeInBytes', 0))}</span>
                    </div>
                </div>

                <div class="operators">
                    <div class="operators-label">Pipelines & Operators:</div>
        """

        # Group operators by pipeline
        pipelines = {}
        for op in stage["operators"]:
            pipeline_id = op.get("pipeline_id", 0)
            if pipeline_id not in pipelines:
                pipelines[pipeline_id] = []
            pipelines[pipeline_id].append(op)

        # Build operator type lookup map for all operators in all stages
        operator_type_map = {}  # (stage_id, pipeline_id, operator_id) -> operator_type
        for s in stages:
            for op in s.get("operators", []):
                key = (s["stageId"], op.get("pipeline_id", 0), op.get("operator_id", "?"))
                operator_type_map[key] = op.get("name", "Unknown")

        # Display pipelines in order
        for pipeline_id in sorted(pipelines.keys()):
            stages_html += f"""
                    <div class="pipeline">
                        <div class="pipeline-header">Pipeline {pipeline_id}</div>
            """

            # Display operators within pipeline (reversed - 0 at bottom)
            for op in reversed(pipelines[pipeline_id]):
                op_type = op.get("name", "Unknown")
                operator_id = op.get("operator_id", "?")
                plan_node_id = op.get("plan_node_id", "N/A")
                stage_id = stage["stageId"]

                node_label = f'<span class="operator-node-label">Node {plan_node_id}</span>'

                # Build operator statistics section
                stats_html = ""
                input_size = op.get("input_data_size", 0)
                output_size = op.get("output_data_size", 0)
                cpu_time = op.get("cpu_time", "N/A")
                wall_time = op.get("wall_time", "0.00ns")
                raw_input_size = op.get("raw_input_data_size", 0)
                input_positions = op.get("input_positions", 0)
                output_positions = op.get("output_positions", 0)

                # Only show stats if we have meaningful data
                if input_size > 0 or output_size > 0 or cpu_time != "N/A" or wall_time != "0.00ns":
                    stats_html = f"""
                    <div class="operator-stats">
                        <div class="stat-row">
                            <span class="stat-label">Input:</span>
                            <span class="stat-value">{format_data_size(input_size)} / {format_rows(input_positions)} rows</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Output:</span>
                            <span class="stat-value">{format_data_size(output_size)} / {format_rows(output_positions)} rows</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Wall Time:</span>
                            <span class="stat-value">{wall_time}</span>
                        </div>
                    </div>
                    """

                # Check if this operator has connections (bi-directional)
                connection_html = ""
                # Connection key now includes pipeline ID: (stage, pipeline, operator_id)
                connection_key = (stage_id, pipeline_id, operator_id)
                if connection_key in connections:
                    # connections[key] is now a list of [(dest_key, direction), ...]
                    for (linked_stage, linked_pipeline, linked_op_id), direction in connections[connection_key]:
                        anchor_id = f"stage-{linked_stage}-p{linked_pipeline}-op-{linked_op_id}"

                        # Look up the operator type for the linked operator
                        linked_key = (linked_stage, linked_pipeline, linked_op_id)
                        linked_op_type = operator_type_map.get(linked_key, "Unknown")

                        # Determine link symbol and text based on direction and stage
                        if direction == 'forward':
                            # This operator SENDS data
                            link_symbol = "→"
                            link_text = f"{link_symbol} Stage {linked_stage}, Pipeline {linked_pipeline}, Op {linked_op_id} ({linked_op_type})"
                            link_class = "operator-link-forward"
                        else:  # direction == 'reverse'
                            # This operator RECEIVES data
                            link_symbol = "←"
                            link_text = f"{link_symbol} Stage {linked_stage}, Pipeline {linked_pipeline}, Op {linked_op_id} ({linked_op_type})"
                            link_class = "operator-link-reverse"

                        connection_html += f'<div class="operator-link {link_class}"><a href="#{anchor_id}">{link_text}</a></div>'

                stages_html += f"""
                        <div class="operator" id="stage-{stage_id}-p{pipeline_id}-op-{operator_id}">
                            <div class="operator-header">
                                <div class="operator-name">{op_type} <span class="operator-ids">[{stage_id}, {pipeline_id}, {operator_id}]</span></div>
                                <span class="operator-num">Op {operator_id}</span>
                            </div>
                            <div class="operator-details">{node_label}</div>
                            {stats_html}
                            {connection_html}
                        </div>
                """

            stages_html += """
                    </div>
            """

        stages_html += """
                </div>
            </div>
        </div>
        """

        # Add arrow between stages (except after last stage)
        if idx < len(stages) - 1:
            stages_html += '<div class="stage-arrow">↓</div>'

    # Build summary stats (merged operator summary is now above stages)
    summary_html = f"""
    <div class="summary-grid">
        <div class="summary-item"><strong>Tasks:</strong> {query_info.get('totalTasks', 'N/A')}</div>
        <div class="summary-item"><strong>Drivers:</strong> {query_info.get('totalDrivers', 'N/A')}</div>
        <div class="summary-item"><strong>Peak Memory:</strong> {query_info.get('peakUserMemoryReservation', 'N/A')}</div>
        <div class="summary-item"><strong>CPU Time:</strong> {query_info.get('totalCpuTime', 'N/A')}</div>
        <div class="summary-item"><strong>Scheduled Time:</strong> {query_info.get('totalScheduledTime', 'N/A')}</div>
        <div class="summary-item"><strong>Planning Time:</strong> {query_info.get('totalPlanningTime', 'N/A')}</div>
        <div class="summary-item"><strong>Execution Time:</strong> {query_info.get('executionTime', 'N/A')}</div>
        <div class="summary-item"><strong>Raw Input Data:</strong> {format_data_size(query_info.get('rawInputDataSize', 0))}</div>
        <div class="summary-item"><strong>Shuffled Data:</strong> {format_data_size(query_info.get('shuffledDataSize', 0))}</div>
        <div class="summary-item"><strong>Output Data:</strong> {format_data_size(query_info.get('outputDataSize', 0))}</div>
    </div>
    """

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Query Plan - {query_id}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            background-color: #f5f5f5;
            color: #333;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
        }}

        header {{
            background: white;
            border-radius: 4px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        h1 {{
            font-size: 24px;
            margin-bottom: 10px;
            color: #222;
        }}

        .query-info {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 15px;
        }}

        .query-id {{
            font-family: monospace;
            font-size: 12px;
            color: #666;
            background: #f9f9f9;
            padding: 4px 8px;
            border-radius: 3px;
        }}

        .state-badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 3px;
            font-size: 12px;
            font-weight: bold;
            color: white;
            margin-left: 8px;
        }}

        .query-text {{
            background: #f9f9f9;
            border-left: 3px solid #3498db;
            padding: 12px;
            margin-top: 10px;
            border-radius: 3px;
            font-family: monospace;
            font-size: 13px;
            color: #555;
            overflow-x: auto;
            word-break: break-all;
        }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-top: 20px;
        }}

        .summary-item {{
            background: white;
            padding: 12px;
            border-radius: 4px;
            border-left: 3px solid #3498db;
            font-size: 13px;
        }}

        .summary-item strong {{
            color: #3498db;
            display: block;
            margin-bottom: 4px;
        }}

        .operators-summary {{
            background: white;
            padding: 20px;
            border-radius: 4px;
            margin-top: 20px;
            border-top: 3px solid #2ecc71;
        }}

        .operators-summary-title {{
            font-weight: bold;
            font-size: 16px;
            margin-bottom: 15px;
            color: #222;
            padding-bottom: 10px;
            border-bottom: 2px solid #e8f4f8;
        }}

        .operators-summary-list {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
        }}

        .operator-summary-item {{
            background: #f9f9f9;
            padding: 12px;
            border-radius: 4px;
            border-left: 3px solid #2ecc71;
        }}

        .operator-summary-name {{
            font-weight: bold;
            color: #222;
            font-size: 14px;
            margin-bottom: 6px;
        }}

        .operator-summary-stages {{
            font-size: 12px;
            color: #666;
            font-family: monospace;
        }}

        main {{
            background: white;
            border-radius: 4px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        .stages {{
            display: flex;
            flex-direction: column;
            gap: 0;
        }}

        .stage {{
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-bottom: 15px;
            overflow: hidden;
            background: white;
        }}

        .stage-header {{
            padding: 12px 16px;
            color: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }}

        .stage-title {{
            font-size: 16px;
            font-weight: bold;
        }}

        .stage-id {{
            font-size: 12px;
            opacity: 0.9;
        }}

        .stage-content {{
            padding: 16px;
            background: #fafafa;
        }}

        .stage-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-bottom: 16px;
            padding-bottom: 16px;
            border-bottom: 1px solid #eee;
        }}

        .stat-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 13px;
        }}

        .stat-label {{
            color: #666;
            font-weight: 500;
        }}

        .stat-value {{
            color: #333;
            font-weight: bold;
            font-family: monospace;
        }}

        .operators-label {{
            font-weight: bold;
            color: #333;
            margin-bottom: 8px;
            font-size: 13px;
        }}

        .operators {{
            display: flex;
            flex-direction: column;
            gap: 15px;
        }}

        .pipeline {{
            border: 1px solid #e0e0e0;
            border-radius: 4px;
            padding: 12px;
            background-color: #fafafa;
        }}

        .pipeline-header {{
            font-weight: bold;
            color: #3498db;
            font-size: 13px;
            margin-bottom: 10px;
            padding-bottom: 8px;
            border-bottom: 2px solid #3498db;
        }}

        .operator {{
            background: white;
            border: 1px solid #ddd;
            border-left: 3px solid #2ecc71;
            padding: 10px;
            border-radius: 3px;
            font-size: 12px;
            margin-bottom: 8px;
        }}

        .operator:last-child {{
            margin-bottom: 0;
        }}

        .operator-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 8px;
            margin-bottom: 6px;
        }}

        .operator-name {{
            font-weight: bold;
            color: #222;
            flex: 1;
            display: flex;
            align-items: baseline;
            gap: 8px;
        }}

        .operator-ids {{
            font-weight: normal;
            font-size: 11px;
            color: #666;
            font-family: monospace;
            background-color: #f0f0f0;
            padding: 1px 4px;
            border-radius: 2px;
            white-space: nowrap;
        }}

        .operator-num {{
            background-color: #e8f4f8;
            color: #0277bd;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 10px;
            font-weight: bold;
            font-family: monospace;
            white-space: nowrap;
            flex-shrink: 0;
        }}

        .operator-id-label {{
            background-color: #e8f4f8;
            color: #0277bd;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 10px;
            font-weight: bold;
            font-family: monospace;
            white-space: nowrap;
            flex-shrink: 0;
        }}

        .operator-details {{
            color: #666;
            font-family: monospace;
            font-size: 11px;
            word-break: break-all;
        }}

        .operator-stats {{
            background-color: #f9f9f9;
            border: 1px solid #e0e0e0;
            border-radius: 3px;
            padding: 8px;
            margin-top: 8px;
            margin-bottom: 8px;
        }}

        .operator-stats .stat-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 12px;
            margin-bottom: 4px;
        }}

        .operator-stats .stat-row:last-child {{
            margin-bottom: 0;
        }}

        .operator-stats .stat-label {{
            color: #555;
            font-weight: 500;
            flex: 0 0 auto;
            margin-right: 8px;
        }}

        .operator-stats .stat-value {{
            color: #222;
            font-weight: bold;
            font-family: monospace;
            font-size: 11px;
            flex: 1;
            text-align: right;
        }}

        .operator-node-label {{
            background-color: #f0f0f0;
            color: #666;
            padding: 2px 4px;
            border-radius: 2px;
            font-size: 10px;
            font-family: monospace;
        }}

        .operator-link {{
            margin-top: 8px;
            padding-top: 8px;
            border-top: 1px dotted #ddd;
            font-size: 12px;
        }}

        .operator-link a {{
            color: #e74c3c;
            font-weight: bold;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }}

        .operator-link a:hover {{
            text-decoration: underline;
            color: #c0392b;
        }}

        .operator-link-forward a {{
            color: #2ecc71;
            font-weight: bold;
        }}

        .operator-link-forward a:hover {{
            color: #27ae60;
        }}

        .operator-link-reverse a {{
            color: #3498db;
            font-weight: bold;
        }}

        .operator-link-reverse a:hover {{
            color: #2980b9;
        }}

        .remote-source {{
            color: #e74c3c;
            font-weight: bold;
            display: block;
            margin-top: 4px;
        }}

        .stage-arrow {{
            text-align: center;
            font-size: 24px;
            color: #3498db;
            margin: 10px 0;
            font-weight: bold;
        }}

        main {{
            display: flex;
            gap: 20px;
        }}

        .stage-nav {{
            width: 200px;
            flex-shrink: 0;
            background: white;
            border-radius: 4px;
            padding: 15px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            max-height: calc(100vh - 60px);
            position: sticky;
            top: 20px;
            overflow-y: auto;
            overflow-x: hidden;
        }}

        /* Custom scrollbar styling */
        .stage-nav::-webkit-scrollbar {{
            width: 8px;
        }}

        .stage-nav::-webkit-scrollbar-track {{
            background: transparent;
        }}

        .stage-nav::-webkit-scrollbar-thumb {{
            background: #d0d0d0;
            border-radius: 4px;
        }}

        .stage-nav::-webkit-scrollbar-thumb:hover {{
            background: #999;
        }}

        /* Firefox scrollbar */
        .stage-nav {{
            scrollbar-color: #d0d0d0 transparent;
            scrollbar-width: thin;
        }}

        .stage-nav-title {{
            font-weight: bold;
            font-size: 14px;
            margin-bottom: 12px;
            color: #333;
            padding-bottom: 8px;
            border-bottom: 2px solid #3498db;
        }}

        .stage-nav-list {{
            display: flex;
            flex-direction: column;
            gap: 6px;
        }}

        .stage-nav-link {{
            padding: 8px 10px;
            border-radius: 3px;
            font-size: 13px;
            color: #3498db;
            text-decoration: none;
            border-left: 3px solid transparent;
            transition: all 0.2s;
        }}

        .stage-nav-link:hover {{
            background-color: #e8f4f8;
            border-left-color: #3498db;
            padding-left: 12px;
        }}

        .stages {{
            flex: 1;
        }}

        @media (max-width: 768px) {{
            .query-info {{
                flex-direction: column;
                align-items: flex-start;
            }}

            .stage-header {{
                flex-direction: column;
                align-items: flex-start;
            }}

            .stage-stats {{
                grid-template-columns: 1fr;
            }}

            .operators {{
                grid-template-columns: 1fr;
            }}

            main {{
                flex-direction: column;
            }}

            .stage-nav {{
                width: 100%;
                position: relative;
                top: auto;
                margin-bottom: 20px;
            }}
        }}

        footer {{
            text-align: center;
            margin-top: 20px;
            color: #999;
            font-size: 12px;
        }}

        .operator-summary {{
            background: white;
            border-radius: 4px;
            padding: 15px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            overflow-x: auto;
        }}

        .operator-summary-title {{
            font-weight: bold;
            font-size: 14px;
            margin-bottom: 12px;
            color: #333;
            padding-bottom: 8px;
            border-bottom: 2px solid #e74c3c;
        }}

        .operator-summary-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            min-width: 600px;
        }}

        .operator-summary-table thead {{
            background-color: #f5f5f5;
            border-bottom: 2px solid #ddd;
            position: sticky;
            top: 0;
        }}

        .operator-summary-table th {{
            padding: 10px 12px;
            text-align: left;
            font-weight: bold;
            color: #333;
            white-space: nowrap;
        }}

        .operator-summary-table tbody tr {{
            border-bottom: 1px solid #eee;
            transition: background-color 0.2s;
        }}

        .operator-summary-table tbody tr:hover {{
            background-color: #f9f9f9;
        }}

        .operator-summary-table td {{
            padding: 10px 12px;
            vertical-align: middle;
        }}

        .operator-summary-table td:nth-child(1) {{
            font-weight: 500;
            color: #222;
            max-width: 200px;
        }}

        .operator-summary-table td:nth-child(2) {{
            text-align: center;
            font-family: monospace;
            color: #0277bd;
        }}

        .operator-summary-table td:nth-child(3) {{
            text-align: right;
            font-family: monospace;
            color: #e74c3c;
            font-weight: bold;
        }}

        .operator-summary-table td:nth-child(4) {{
            font-family: monospace;
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Query Execution Plan</h1>
            <div class="query-info">
                <div class="query-id">Query ID: {query_id}</div>
                <div class="state-badge" style="background-color: {state_color}">{state}</div>
            </div>
            <div class="query-text"><strong>Query:</strong> {query_text}</div>
            {summary_html}
            {operator_summary_html}
        </header>

        <main>
            {stage_nav_html}
            <div class="stages">
                {stages_html}
            </div>
        </main>

        <footer>
            <p>Generated from Presto query execution plan • Query Plan Simple Visualizer</p>
        </footer>
    </div>
</body>
</html>
"""

    return html


def main():
    parser = argparse.ArgumentParser(
        description="Generate simple, readable query plan visualization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s order_nex_query_2026_02_09.json
  %(prog)s order_nex_query_2026_02_09.json --output plan.html
        """
    )

    parser.add_argument("input", help="Path to query plan JSON file")
    parser.add_argument("-o", "--output", help="Output HTML file")

    args = parser.parse_args()

    # Read input JSON
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(input_path) as f:
            query_plan = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file: {e}", file=sys.stderr)
        sys.exit(1)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / f"{input_path.stem}_simple.html"

    # Generate HTML
    print(f"Generating simple visualization from {input_path}...", file=sys.stderr)
    html_content = generate_html(query_plan)

    # Write output
    try:
        with open(output_path, "w") as f:
            f.write(html_content)
        print(f"✓ Visualization saved to: {output_path}", file=sys.stderr)
    except IOError as e:
        print(f"Error: Failed to write output file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
