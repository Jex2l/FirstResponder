import duckdb
con = duckdb.connect('data/responder.duckdb', read_only=True)

print('=== Manhattan 5th Avenue near 350 ===')
for r in con.execute("""
    SELECT address, borough, zipcode, num_floors, year_built
    FROM buildings
    WHERE borough = 'MN'
      AND (address LIKE '%5 AVENUE%' OR address LIKE '%FIFTH%' OR address LIKE '%5TH AVENUE%')
      AND address LIKE '3%'
    LIMIT 10
""").fetchall():
    print(r)

print('\n=== All MN addresses starting with 350 ===')
for r in con.execute("""
    SELECT address, borough, zipcode, num_floors
    FROM buildings
    WHERE borough = 'MN' AND address LIKE '350%'
    LIMIT 10
""").fetchall():
    print(r)

print('\n=== Street name formats for avenues ===')
for r in con.execute("""
    SELECT DISTINCT address FROM buildings
    WHERE address LIKE '%AVENUE%' AND borough = 'MN'
    LIMIT 5
""").fetchall():
    print(r)

con.close()