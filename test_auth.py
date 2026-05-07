#!/usr/bin/env python3
"""
Quick test: Authenticate to RSC and list all discovered clusters.
Tries multiple query formats for compatibility with RSC and RSC-P.
"""
import urllib3
urllib3.disable_warnings()

from config import Config, setup_logging
from rsc_client import RSCClient, RSCClientError

logger = setup_logging("INFO")


def main():
    print("")
    print("=" * 60)
    print("RSC Authentication & Cluster Discovery Test")
    print("=" * 60)
    print("")

    if not Config.validate():
        print("Fix your .env file and try again.")
        return 1

    print(f"RSC URL: {Config.RSC_BASE_URL}")
    print(f"Client ID: {Config.RSC_CLIENT_ID[:12]}...")
    print("")

    # Authenticate
    try:
        client = RSCClient()
        client.authenticate()
        print("RSC Authentication successful!")
        print("")
    except RSCClientError as e:
        print(f"AUTHENTICATION FAILED: {e}")
        return 1

    # Try multiple query formats
    queries_to_try = [
        (
            "clusterConnection (standard)",
            """
            query {
                clusterConnection(first: 20) {
                    count
                    edges {
                        node {
                            id
                            name
                            version
                            status
                            type
                            defaultAddress
                            state { connectedState }
                            clusterNodeConnection(first: 1) {
                                count
                            }
                        }
                    }
                }
            }
            """,
            "clusterConnection",
        ),
        (
            "clusterConnection (minimal)",
            """
            query {
                clusterConnection(first: 20) {
                    count
                    edges {
                        node {
                            id
                            name
                            version
                            status
                        }
                    }
                }
            }
            """,
            "clusterConnection",
        ),
        (
            "clusterConnection with variables",
            """
            query ListClusters($first: Int) {
                clusterConnection(first: $first) {
                    count
                    edges {
                        node {
                            id
                            name
                            version
                            status
                        }
                    }
                }
            }
            """,
            "clusterConnection",
        ),
        (
            "allClusterConnection",
            """
            query {
                allClusterConnection(first: 20) {
                    count
                    edges {
                        node {
                            id
                            name
                            version
                            status
                        }
                    }
                }
            }
            """,
            "allClusterConnection",
        ),
    ]

    success = False
    for query_name, query, data_key in queries_to_try:
        print(f"Trying: {query_name}...")

        if "variables" in query_name.lower():
            result = client.graphql(
                query, variables={"first": 20}
            )
        else:
            result = client.graphql(query)

        conn = result.get(data_key, {})
        edges = conn.get("edges", [])
        total = conn.get("count", 0)

        if edges:
            print(f"  SUCCESS! Found {total} clusters.")
            print("")
            _print_cluster_table(edges)
            success = True
            break
        elif total > 0:
            print(f"  Got count={total} but no edges.")
        else:
            print(f"  No data returned.")
        print("")

    if not success:
        print("")
        print("=" * 60)
        print("NONE OF THE QUERIES RETURNED DATA")
        print("=" * 60)
        print("")
        print("Possible causes:")
        print("  1. Service account may not have cluster access")
        print("  2. RSC-P schema may differ from standard RSC")
        print("  3. No CDM clusters registered to this RSC instance")
        print("")
        print("Run debug_queries.py for detailed schema inspection:")
        print("  python3 debug_queries.py")
        print("")

        # Try schema introspection
        print("Checking available query fields...")
        schema_result = client.graphql("""
            query {
                __schema {
                    queryType {
                        fields {
                            name
                        }
                    }
                }
            }
        """)
        schema = schema_result.get("__schema", {})
        query_type = schema.get("queryType", {})
        fields = query_type.get("fields", [])

        if fields:
            # Find cluster-related queries
            cluster_fields = [
                f["name"] for f in fields
                if "cluster" in f["name"].lower()
            ]
            print(f"  Found {len(fields)} total query fields.")
            print(f"  Cluster-related fields:")
            for cf in cluster_fields:
                print(f"    - {cf}")
            print("")
            print(
                "Update the queries in cluster_discovery.py "
                "to use one of these field names."
            )
        else:
            print("  Schema introspection also returned no data.")
            print("  Service account may lack read permissions.")

        return 1

    # Print config summary
    print("")
    if Config.TARGET_CDM_VERSION:
        print(
            f"Target CDM Version: {Config.TARGET_CDM_VERSION}"
        )
    else:
        print(
            "Target CDM Version: Not set "
            "(will use latest in matrix)"
        )
    if Config.CDM_CLUSTER_IDS:
        print(f"Cluster ID Filter: {Config.CDM_CLUSTER_IDS}")
    if Config.CDM_CLUSTER_NAMES:
        print(
            f"Cluster Name Filter: {Config.CDM_CLUSTER_NAMES}"
        )
    if Config.SKIP_DISCONNECTED:
        print("Skip Disconnected: Yes")

    print("")
    print("=" * 60)
    print("Ready to run full assessment!")
    print("  python3 main.py")
    print("  OR")
    print("  ./run.sh")
    print("=" * 60)
    print("")

    return 0


def _print_cluster_table(edges):
    """Print a formatted table of clusters."""
    print(
        f"  {'Cluster':<28} {'Version':<12} "
        f"{'Type':<12} {'State':<15} {'Nodes'}"
    )
    print("  " + "-" * 75)

    for edge in edges:
        n = edge.get("node", {})
        name = n.get("name", "?")
        version = n.get("version", "?")
        ctype = n.get("type", "?")
        state = n.get("state", {})
        if isinstance(state, dict):
            state = state.get("connectedState", "?")
        nodes = "?"
        node_conn = n.get("clusterNodeConnection", {})
        if isinstance(node_conn, dict):
            nodes = node_conn.get("count", "?")

        if state == "Connected":
            indicator = "OK"
        elif state == "DISCONNECTED":
            indicator = "DC"
        else:
            indicator = "??"

        print(
            f"  [{indicator}] {name:<24} "
            f"v{version:<10} "
            f"{ctype:<12} "
            f"{state:<15} "
            f"{nodes}"
        )

    print("")


if __name__ == "__main__":
    exit(main())