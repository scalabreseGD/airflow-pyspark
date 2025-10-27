"""
Airflow Plugin to expose REST API endpoints for extracting the dag_code table.
Compatible with Airflow 2.7.0

Simplified version using Airflow's standard authentication.
"""

from airflow.api.auth.backend.basic_auth import requires_authentication as basic_auth
from airflow.models.dagcode import DagCode
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.session import provide_session
from airflow.www.app import csrf
from flask import Blueprint, jsonify, request
from sqlalchemy.orm import Session

# Create a Flask Blueprint for the custom endpoint
dag_code_bp = Blueprint(
    "dag_code_extractor",
    __name__,
    url_prefix="/api/v1/dag_code"
)


@dag_code_bp.route("/extract", methods=["GET"])
@csrf.exempt
@basic_auth
@provide_session
def extract_dag_code(session: Session = None):
    """
    Extract all records from the dag_code table.

    Query Parameters:
    - fileloc: Optional. Filter by specific file location
    - limit: Optional. Limit the number of results (default: 100, max: 1000)
    - offset: Optional. Offset for pagination (default: 0)

    Returns:
        JSON response with dag_code records
    """
    try:
        # Get query parameters
        fileloc = request.args.get('fileloc', None)
        limit = min(request.args.get('limit', 100, type=int), 1000)
        offset = request.args.get('offset', 0, type=int)

        # Build query
        query = session.query(DagCode)

        # Apply filters
        if fileloc:
            query = query.filter(DagCode.fileloc == fileloc)

        # Get total count
        total_count = query.count()

        # Apply pagination
        dag_codes = query.limit(limit).offset(offset).all()

        # Serialize results
        results = []
        for dag_code in dag_codes:
            results.append({
                'fileloc': dag_code.fileloc,
                'fileloc_hash': dag_code.fileloc_hash,
                'source_code': dag_code.source_code,
                'last_updated': dag_code.last_updated.isoformat() if dag_code.last_updated else None
            })

        return jsonify({
            'total_count': total_count,
            'count': len(results),
            'offset': offset,
            'limit': limit,
            'dag_codes': results
        }), 200

    except Exception as e:
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500


@dag_code_bp.route("/extract/<path:fileloc>", methods=["GET"])
@csrf.exempt
@basic_auth
@provide_session
def extract_dag_code_by_fileloc(fileloc: str, session: Session = None):
    """
    Extract a specific dag_code record by file location.

    Args:
        fileloc: The file location path

    Returns:
        JSON response with the dag_code record
    """
    try:
        dag_code = session.query(DagCode).filter(DagCode.fileloc == fileloc).first()

        if not dag_code:
            return jsonify({
                'error': 'Not Found',
                'message': f'No dag_code found for fileloc: {fileloc}'
            }), 404

        result = {
            'fileloc': dag_code.fileloc,
            'fileloc_hash': dag_code.fileloc_hash,
            'source_code': dag_code.source_code,
            'last_updated': dag_code.last_updated.isoformat() if dag_code.last_updated else None
        }

        return jsonify(result), 200

    except Exception as e:
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500


@dag_code_bp.route("/health", methods=["GET"])
@csrf.exempt
def health_check():
    """
    Simple health check endpoint (no authentication required).

    Returns:
        JSON response indicating the plugin is loaded and working
    """
    return jsonify({
        'status': 'healthy',
        'plugin': 'dag_code_extractor',
        'version': '1.0.0'
    }), 200


# Define the Airflow Plugin
class DagCodeExtractorPlugin(AirflowPlugin):
    name = "dag_code_extractor"
    flask_blueprints = [dag_code_bp]
