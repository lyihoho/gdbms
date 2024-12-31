import duckdb
import time

#Connect to the DuckDB database
con = duckdb.connect(database='database.duckdb')
con.execute("INSTALL spatial")
con.execute("LOAD spatial")

#Enable parallel processing
try: 
    con.execute("SET threads = 8;")
    print("Parallel processing enabled with 8 threads.")
except duckdb.CatalogException:
    print("The 'SET threads' parameter is not supported in this version of DuckDB. Proceeding with default settings." )

#Measure execution time
start_time = time.time()

#Execute query with parallel processing
result = con.execute("""
    WITH calculated_distances AS (
        SELECT 
            id_1, 
            id_2, 
            ST_Distance(geom, ST_GeomFromText('LINESTRING(0 0, 10 10)')) AS distance
        FROM edges
    )
    SELECT 
        id_1, 
        COUNT(id_2) AS total_connections, 
        AVG(distance) AS avg_distance, 
        MAX(distance) AS max_distance
    FROM 
        calculated_distances
    WHERE 
        distance > 1000 -- Arbitrary filter for more complexity
    GROUP BY 
        id_1
    HAVING 
        AVG(distance) > 100 -- Another filter on aggregated results
    ORDER BY 
        total_connections DESC
    LIMIT 15;
""").fetchall()

end_time = time.time()

#Output the results and query execution time
print(result)
print("Query execution time with parallel processing: {:.4f} seconds".format(end_time - start_time))