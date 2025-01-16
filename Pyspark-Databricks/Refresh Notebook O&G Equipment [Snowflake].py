# Databricks notebook source
import snowflake.connector
from datetime import datetime, timedelta

def get_oauth_connection():
    """Create Snowflake connection using OAuth"""
    return snowflake.connector.connect(
        account='EO05324',
        authenticator='oauth',
        token=dbutils.secrets.get("snowflake_scope", "oauth_token"),
        warehouse='COMPUTE_WH',
        database='EQUIPMENT_DB',
        schema='GOLD_SCHEMA'
    )

def setup_oauth_secrets(client_id, client_secret):
    """Store OAuth credentials in Databricks secrets"""
    try:
        dbutils.secrets.put("snowflake_scope", "oauth_client_id", client_id)
        dbutils.secrets.put("snowflake_scope", "oauth_client_secret", client_secret)
        print("✓ OAuth credentials stored successfully")
    except Exception as e:
        print(f"Error storing OAuth credentials: {str(e)}")
        raise e

def test_oauth_connection():
    """Test the OAuth connection to Snowflake"""
    try:
        conn = get_oauth_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
        result = cursor.fetchone()
        print(f"Successfully connected as: {result[0]}")
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()