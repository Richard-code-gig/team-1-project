# def insert_customer():
#     try:
#         with connection.cursor() as cursor:
#             sql = """INSERT INTO customer (customer_id, price) values (select customer_id, price from raw_table)"""
#             cursor.execute(sql)
#             connection.commit()
#             cursor.close()
#     except Exception as e:
#         print(e) 