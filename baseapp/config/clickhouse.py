import logging, clickhouse_connect
from baseapp.config import setting
from datetime import datetime

logger = logging.getLogger()

class Column:
    def __init__(self, name, data_type, default_value=None, is_primary_key=False, is_index=False):
        self.name = name
        self.data_type = data_type
        self.default_value = default_value
        self.is_primary_key = is_primary_key
        self.is_index = is_index

    def __str__(self):
        column_def = f"{self.name} {self.data_type}"
        if self.default_value is not None:
            column_def += f" DEFAULT {self.default_value}"
        return column_def
    
    def is_string(self):
        return "String" in self.data_type or "LowCardinality(String)" in self.data_type

    def is_numeric(self):
        return "Int" in self.data_type or "UInt" in self.data_type or "Float" in self.data_type

    def is_date(self):
        return "Date" in self.data_type or "DateTime" in self.data_type

class TableSchema:
    def __init__(self, table_name):
        self.table_name = table_name
        self.columns = []
        self.primary_key_columns = []
        self.index_columns = []

    def add_column(self, name, data_type, default_value=None, is_primary_key=False, is_index=False):
        column = Column(name, data_type, default_value, is_primary_key, is_index)
        self.columns.append(column)
        
        if is_primary_key:
            self.primary_key_columns.append(name)
        
        if is_index:
            self.index_columns.append(column)

    def generate_create_table_query(self):
        columns_str = ",\n".join(str(column) for column in self.columns)
        
        # if field has indexing
        index_str = ""
        for column in self.index_columns:
            if column.is_string():
                index_type = "set(0)"
            elif column.is_numeric() or column.is_date():
                index_type = "minmax"
            else:
                raise ValueError(f"Unknown index type for column '{column.name}' with data type '{column.data_type}'")
            
            index_str += f"INDEX {column.name}_idx ({column.name}) TYPE {index_type} GRANULARITY 3,\n"
        
        # Removing trailing comma from index_str
        index_str = index_str.strip().rstrip(',')
        
        # Create the query
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n{columns_str},\n{index_str}\n) ENGINE = MergeTree"
        
        if self.primary_key_columns:
            query += f" ORDER BY ({', '.join(self.primary_key_columns)})" if self.primary_key_columns else "ORDER BY tuple()"
        
        return query

class ClickHouseConn:
    def __init__(self, host=None, port=None, username=None, password=None, database=None, secure=False, verify=False):
        config = setting.get_settings()
        self.host = host or config.clickhouse_host
        self.port = port or config.clickhouse_port
        self.username = username or config.clickhouse_user
        self.password = password or config.clickhouse_pass
        self.database = database or config.clickhouse_db
        self.secure = secure or config.clickhouse_secure
        self.verify = verify or config.clickhouse_verify
        self._conn = None

    def connect_to_server(self):
        """Establish connection to the ClickHouse server without specifying a database."""
        try:
            self._conn = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                secure=self.secure,
                verify=self.verify
            )
            logger.info("ClickHouse: Connected to server successfully")
        except Exception as e:
            logger.error(f"ClickHouse: Error connecting to server: {e}")
            raise ConnectionError("ClickHouse: Error connecting to server")
    
    def __enter__(self):
        """Establish a connection to the server and select the database."""
        self.connect_to_server()
        return self._conn
    
    def select_database(self, database=None, create_if_missing=False):
        """Select the specified database after connecting to the server."""
        database = database or self.database
        if not self._conn:
            raise ValueError("ClickHouse: Connection to server is not established")
        try:
            self._conn.command(f"USE {database}")
            logger.info(f"ClickHouse: Switched to database '{database}' successfully")
        except Exception as e:
            if "UNKNOWN_DATABASE" in str(e) and create_if_missing:
                try:
                    self._conn.command(f"CREATE DATABASE {database}")
                    logger.info(f"ClickHouse: Database '{database}' created successfully")
                    self._conn.command(f"USE {database}")
                except Exception as db_create_error:
                    logger.error(f"Error creating database '{database}': {db_create_error}")
                    raise ValueError("ClickHouse: error creating database")
            else:
                logger.error(f"ClickHouse: Error switching to database '{database}': {e}")
                raise ValueError("ClickHouse: Error switching to database")

    def get_connection(self):
        """Ensure a connection to the server and return the client."""
        if not self._conn:
            self.connect_to_server()
        return self._conn

    def create_table_with_schema(self, table_schema_obj):
        """Create a table using a schema object."""
        query = table_schema_obj.generate_create_table_query()
        self.execute_no_return(query)
        logger.info(f"ClickHouse: Table '{table_schema_obj.table_name}' created or already exists.")

    def execute_query(self, query, params=None):
        """Execute a query on the ClickHouse database."""
        if self._conn is None:
            logger.info("ClickHouse: Client is not connected. Please call the connect method first.")
            raise ConnectionError("ClickHouse: Client is not connected")
        try:
            result = self._conn.query(query, parameters=params)
            return result
        except Exception as e:
            logger.error(f"ClickHouse: Error executing query: {e}")
            raise ValueError("ClickHouse: Error executing query")

    def execute_no_return(self, query, params=None):
        """Execute a query that doesn't return results (e.g., INSERT)."""
        if self._conn is None:
            logger.info("ClickHouse: Client is not connected. Please call the connect method first.")
            raise ConnectionError("ClickHouse: Client is not connected")
        try:
            self._conn.command(query, parameters=params)
            logger.info("ClickHouse: Query executed successfully")
        except Exception as e:
            logger.error(f"ClickHouse: Error executing query: {e}")
            raise ValueError("ClickHouse: Error executing query")
    
    def insert_data(self, table_name, data, columns):
        """
        Insert data into the specified ClickHouse table using SQL command.
        
        :param table_name: Name of the table
        :param data: List of dictionaries, where each dictionary represents a row of data
        :param columns: List of column names in the table
        """
        # Preparing data for insertion: tuple of tuples, matching the table schema
        insert_values = [
            f"(generateUUIDv4(), {', '.join(self.format_value(row[col]) for col in columns)})"
            for row in data
        ]
    
        # Prepare SQL insert query
        columns_str = ", ".join(['id'] + columns)
        values_str = ", ".join(insert_values)
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_str};"
        
        # Execute the insert query
        self.execute_query(insert_query)

        # print(f"query insert {insert_query}")
        logger.info(f"ClickHouse: Inserted {len(data)} rows into table {table_name}")

    def update_data(self, table_name, update_fields, condition_fields):
        """
        Update data in the table with specified update fields and conditions.
        :param table_name: Name of the table.
        :param update_fields: Dictionary of columns and their new values to update.
        :param condition_fields: Dictionary of columns and their values for the WHERE clause.
        :return: None
        """
        
        # Constructing the SET clause from update_fields
        set_clause = ", ".join([f"{col} = {self.format_value(val)}" for col, val in update_fields.items()])

        # Constructing the WHERE clause from condition_fields
        where_clause = " AND ".join([f"{col} = {self.format_value(val)}" for col, val in condition_fields.items()])

        # Build the mutation query (since ClickHouse uses ALTER TABLE for updates)
        query = f"ALTER TABLE {table_name} UPDATE {set_clause} WHERE {where_clause}"
        
        # Execute the insert query
        self.execute_query(query)

        logger.info(f"ClickHouse: {query}")

    def insert_data_copy(self, table_name, select_into, columns):
        """
        Insert data into the specified ClickHouse table using SQL command.
        
        :param table_name: Name of the table
        :param select_into: Query select data from target table to copy data
        :param columns: List of column names in the table
        """
          
        # Prepare SQL insert query
        columns_str = ", ".join(columns)
        insert_query = f"INSERT INTO {table_name} ({columns_str}) {select_into};"
        
        # Execute the insert query
        self.execute_query(insert_query)

        # print(f"query insert {insert_query}")
        logger.info(f"ClickHouse: Inserted into table {table_name}")
    
    def select_and_count(self, table_name, columns="*", filters=None):
        """
        Select data from the table with optional filters and pagination.
        :param table_name: Name of the table.
        :param columns: Columns to select (default is all columns '*').
        :param filters: Dictionary of filters (optional).
        :return: Result of the SELECT query.
        """
        # Construct the base SELECT query
        query = f"SELECT count({columns}) FROM {table_name}"

        # Add filters if provided
        # if filters:
        #     filter_conditions = [self.build_filter_condition(col, val) for col, val in filters.items()]
        #     query += " WHERE " + " AND ".join(filter_conditions)

        if filters:
            filter_conditions = []
            for column, value in filters.items():
                if isinstance(value, tuple) and len(value) == 2:
                    filter_value, operator = value
                    filter_conditions.append(self.build_filter_condition(column, filter_value, operator))
                else:
                    # Default operator is '=' if no operator is provided
                    filter_conditions.append(self.build_filter_condition(column, value))
            query += " WHERE " + " AND ".join(filter_conditions)

        # logger.debug(f"Query is ... {query}")

        # Execute the query and return results
        result = self.execute_query(query)

         # Access the first row and first column to get the count
        count = result.result_rows[0][0] if result.result_rows else 0

        return count
    
    def select_with_pagination(self, table_name, columns="*", filters=None, offset=0, page_size=10, order_by=None, order_direction="ASC"):
        """
        Select data from the table with optional filters and pagination.
        :param table_name: Name of the table.
        :param columns: Columns to select (default is all columns '*').
        :param filters: Dictionary of filters (optional).
        :param page: Page number for pagination (default is 1).
        :param page_size: Number of records per page (default is 10).
        :return: Result of the SELECT query.
        """
        columns_str = columns
        if isinstance(columns, list):
            columns_str = ", ".join(columns)

        # Construct the base SELECT query
        query = f"SELECT {columns_str} FROM {table_name}"

        if filters:
            filter_conditions = []
            for column, value in filters.items():
                if isinstance(value, tuple) and len(value) == 2:
                    filter_value, operator = value
                    filter_conditions.append(self.build_filter_condition(column, filter_value, operator))
                else:
                    # Default operator is '=' if no operator is provided
                    filter_conditions.append(self.build_filter_condition(column, value))
            query += " WHERE " + " AND ".join(filter_conditions)

        # Add ORDER BY if provided
        if order_by:
            if isinstance(order_by, str):
                query += f" ORDER BY {order_by} {order_direction}"
            elif isinstance(order_by, list):
                query += f" ORDER BY {', '.join([f'{col} {order_direction}' for col in order_by])}"

        # Add pagination using LIMIT and OFFSET
        if offset is not None:
            query += f" LIMIT {page_size} OFFSET {offset};"

        # Execute the query and return results
        result = self.execute_query(query)
    
        # Extract column names from the result
        columns = result.column_names

        # Convert each row to a dictionary
        result_array = [dict(zip(columns, row)) for row in result.result_rows]
        return result_array
    
    def select_with_deduplicate_cond(self, table_name, table_name_2, columns="*", filters=None):
        """
        Select data from the table with optional filters and pagination.
        :param table_name: Name of the table.
        :param table_name_2: Name of the table to copy data.
        :param columns: Columns to select (default is all columns '*').
        :param filters: Dictionary of filters (optional).
        :return: Result of the SELECT query.
        """

        columns_str = columns
        if isinstance(columns, list):
            columns_str = ", ".join(columns)

        # Construct the base SELECT query
        query = f"SELECT {columns_str} FROM {table_name} WHERE id NOT IN (SELECT id FROM {table_name_2})"

        if filters:
            filter_conditions = []
            for column, value in filters.items():
                if isinstance(value, tuple) and len(value) == 2:
                    filter_value, operator = value
                    filter_conditions.append(self.build_filter_condition(column, filter_value, operator))
                else:
                    # Default operator is '=' if no operator is provided
                    filter_conditions.append(self.build_filter_condition(column, value))
            
            query += " AND " + " AND ".join(filter_conditions)

        return query

    def build_filter_condition(self, column, value, operator="="):
        """
        Build filter condition for a specific column and value.
        :param column: Column name
        :param value: Value to filter by. Can be string, date, integer, float, or range.
        :return: Filter condition as a string.
        """
        # Adjust operator based on input (default is '=')
        if operator not in ["=", "!=", "LIKE", "NOT LIKE", "IN", "NOT IN"]:
            logger.error(f"Unsupported operator: {operator}")
            # return None
            raise ValueError(f"Unsupported operator: {operator}")
        
        # Handle IN operator for lists or tuples
        if operator == 'IN' and isinstance(value, (list, tuple)):
            # Assuming value contains a list/tuple of strings
            formatted_values = ", ".join(self.format_value(v) for v in value)
            return f"{column} IN ({formatted_values})"

        # Handle NOT IN operator for lists or tuples
        if operator == 'NOT IN' and isinstance(value, (list, tuple)):
            formatted_values = ", ".join(self.format_value(v) for v in value)
            return f"{column} NOT IN ({formatted_values})"
    
        # Check for LIKE pattern (for strings)
        if isinstance(value, str):
            if "%" in value and operator in ["LIKE", "NOT LIKE"]:
                return f"{column} {operator} {self.format_value(value)}"
            else:
                return f"{column} {operator} {self.format_value(value)}"
            
        # Check for datetime range (tuple or list)
        elif isinstance(value, (tuple, list)) and len(value) == 2 and isinstance(value[0], datetime):
            start_dt = value[0].strftime('%Y-%m-%d %H:%M:%S')
            end_dt = value[1].strftime('%Y-%m-%d %H:%M:%S')
            if operator == "!=":
                return f"NOT (toDateTime({column}) BETWEEN toDateTime('{start_dt}') AND toDateTime('{end_dt}'))"
            return f"toDateTime({column}) BETWEEN toDateTime('{start_dt}') AND toDateTime('{end_dt}')"
        
        # Check for specific datetime
        elif isinstance(value, datetime):
            return f"toDateTime({column}) {operator} toDateTime({self.format_value(value.strftime('%Y-%m-%d %H:%M:%S'))})"
        
        # Check for range of numbers (tuple or list for int or float)
        elif isinstance(value, (tuple, list)) and len(value) == 2 and (isinstance(value[0], (int, float))):
            if operator == "!=":
                return f"NOT ({column} BETWEEN {value[0]} AND {value[1]})"
            return f"{column} BETWEEN {value[0]} AND {value[1]}"
        
        # Check for integer or float
        elif isinstance(value, (int, float)):
            return f"{column} {operator} {value}"
        
        # Default to equality check
        return f"{column} {operator} {self.format_value(value)}"
    
    def format_value(self, value):
        """
        Format the value to be inserted into the query, handling strings and other types.
        """
        if isinstance(value, str):
            # Escape single quotes by replacing ' with ''
            value = value.replace("'", "''")
            return f"'{value}'"
        return str(value)
    
    def close(self):
        """Close the connection to the ClickHouse server."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("ClickHouse: Connection closed")

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

# Example usage:
if __name__ == "__main__":
    client = ClickHouseConn(
        host='172.1.0.21',
        port=9000,
        username='county',
        password='cDBS38zv6k2EwbEO',
        database='default'
    )
    client.connect()

    # Create a new database
    client.create_database('iot')

    # Switch to the new 'iot' database
    client.use_database('iot')  # This will reconnect to 'iot'
    
    # Create a new table
    table_schema = TableSchema("test_demo")
    table_schema.add_column("id", "UInt32", is_primary_key=True)
    table_schema.add_column("_cd", "Date", "today()")
    table_schema.add_column("_cb", "String", is_index=True)
    table_schema.add_column("org_id", "String", is_index=True)
    table_schema.add_column("client_id", "String", is_index=True)
    table_schema.add_column("sub_client_id", "String", is_index=True)

    table_schema.add_column("fullname", "String")
    table_schema.add_column("hobby", "String")
    table_schema.add_column("job", "String")

    table_schema.add_column("age", "Float32", 0)

    create_table_query = table_schema.generate_create_table_query()
    logger.info(create_table_query)

    # Create the table using the schema object
    client.create_table_with_schema(table_schema)

    # Example query
    result = client.execute_query("SELECT * FROM test_demo LIMIT 10")
    if result:
        for row in result.result_rows:
            logger.info(row)

    # Close the connection
    client.close()
