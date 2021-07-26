####10
import json
from pymongo import MongoClient
import csv
from datetime import datetime
import requests
import dropbox
import psycopg2
import sys
import re
from sqlalchemy import create_engine
import pandas as pd


# Add Portal related Functions

# snapdeal portal functions
def get_snapdeal_accounts(cur_products):
    print('fetch accounts')
    cur_products.execute("Select * from master_portalaccount where portal_name = 'snapdealp'")
    portal = cur_products.fetchone()
    accounts = []
    if portal is not None:
        portal_id = portal[0]
        # hardcoded account_id = 11
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()
        print("portal_accounts==>",portal_accounts)
        if len(portal_accounts):
            for portal_account in portal_accounts:
                xauthtoken = ''
                xsellerauthztoken = ''
                client_id = ''
                access_token_field = portal_account[4]
                for item in access_token_field:
                    if item['name'] == 'xauthtoken':
                        xauthtoken = item['base_url']
                    elif item['name'] == 'xsellerauthztoken':
                        xsellerauthztoken = item['base_url']
                    elif item['name'] == 'clientid':
                        client_id = item['base_url']
                warehouses = []
                # print("xauthtoken==",xauthtoken)
                # print("xsellerauthztoken==", xsellerauthztoken)
                # print("client_id==", client_id)
                warehouse_field = portal_account[5]
                for item in warehouse_field:
                    warehouse = {
                        'warehouse_id': item['warehouse_id'],
                        'warehouse_code': item['portal_warehouse'],
                        'xsellerauthztoken': xsellerauthztoken
                    }
                    warehouses.append(warehouse)
                # print("===warehouses",warehouses)
                account = {
                    "portal_name": portal[1],
                    "account_name": portal_account[1],
                    "portal_id": portal_id,
                    "account_id": portal_account[0],
                    "xauthtoken":xauthtoken,
                    "client_id":client_id,
                    "warehouses": warehouses
                }
                accounts.append(account)
                print("account==",account)
    return accounts


def get_snapdeal_product(cur_products, product_id):
    product_query = "SELECT snapdealp_portal_sku, snapdealp_portal_unique_id, snapdealp_account_id from snapdealp_snapdealpproducts where product_id = " + str(product_id)
    cur_products.execute(product_query)
    product = cur_products.fetchone()
    if product is not None:
        return {
            'sku': product[0],
            'unique_id': product[1],
            'account_id': product[2]
        }
    else:
        return None


def get_snapdeal_stock(supc, account, xsellerauthztoken):
    url = 'https://apigateway.snapdeal.com/seller-api/products/'+ str(supc) +'/inventory'
    xsellerauthztoken1='9381cf6a-b03e-4ba9-a098-3584e124439b'
    print("xsellerauthztoken1===",xsellerauthztoken1)
    print("xsellerauthztoken===", xsellerauthztoken)
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId':account['client_id']
    }
    # print("get_snapdeal_stock===headers===",headers)
    # response = requests.get(url, headers=headers).json()
    # print(response)
    # return response
    # print("get_snapdeal_stock===headers===",headers)
    try:
        response = requests.get(url, headers=headers).json()
    except:
        response = 'None'

    # print("get_snapdeal_stock_response==250==>", response)
    return response


def update_snapdeal_stock(set, account, xsellerauthztoken):
    url = 'https://apigateway.snapdeal.com/seller-api/products/inventory'
    xsellerauthztoken1 = '9381cf6a-b03e-4ba9-a098-3584e124439b'
    print("xsellerauthztoken1===", xsellerauthztoken1)
    print("xsellerauthztoken===", xsellerauthztoken)
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId': account['client_id'],
        'Content-Type': 'application/json'
    }
    print(headers)
    # set1={'supc': 'SDL390613676', 'availableInventory': 14}
    print("update_snapdeal===set",set)
    print("update_snapdeal===xsellerauthztoken", xsellerauthztoken)
    print("update_snapdeal===account", account)
    response = requests.post(url, data=json.dumps(set), headers=headers).json()
    print("update_snapdeal_stock==response=>>>>",response)
    return response

def verify_stock_update(account, xsellerauthztoken, uploadid, page):
    # import time
    # print("sleep for 3 mint")
    # time.sleep(180)
    print('uploadid==',uploadid)
    url = 'http://apigateway.snapdeal.com/seller-api/feed/result/' + str(uploadid) + '?pageNumber=' + str(page)
    print(url)
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId': account['client_id']
    }
    response = requests.get(url, headers=headers).json()
    print("verify_stock_update_respmse>>",response)
    return response

# Todo: Add new portal functions here


def main():
    print('Create DB Connections')
    client = MongoClient(
        'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
    db = client.wms
    conn_products = psycopg2.connect(database="products", user="postgres", password="buymore2",
                                     host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_products = conn_products.cursor()
    conn_orders = psycopg2.connect(database="orders", user="postgres", password="buymore2",
                                   host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_orders = conn_orders.cursor()

    # # Create files for the logs of the stock update
    # master files
    master_filename = 'master_refresh_update20_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    master_filefrom = '/tmp/' + master_filename
    master_fileto = '/buymore2/bin_reco/master_stock/' + master_filename
    master_update_csv = open(master_filefrom, 'w')
    master_fieldnames = ['buymore_sku', 'product_id', 'warehouse', 'stock', 'status', 'error']
    master_writer = csv.DictWriter(master_update_csv, fieldnames=master_fieldnames)
    master_writer.writeheader()

    # snapdeal files
    snapdeal_filename = 'master_refresh_snapdeal_update20_' + str(
        int(datetime.timestamp(datetime.now()))) + '.csv'
    snapdeal_filefrom = '/tmp/' + snapdeal_filename
    snapdeal_fileto = '/buymore2/bin_reco/master_stock/' + snapdeal_filename
    snapdeal_update_csv = open(snapdeal_filefrom, 'w')
    snapdeal_fieldnames = ['product_id', 'quantity', 'message', 'status']
    snapdeal_writer = csv.DictWriter(snapdeal_update_csv, fieldnames=snapdeal_fieldnames)
    snapdeal_writer.writeheader()

    # Todo: Add new stock file here

    # fetch account data

    # snapdeal account
    print('Get snapdeals accounts')
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    # Todo: Add fetch account code for new portal here
    # Fetch the stocks from the WMS
    pipeline = [
        {
            "$match": {
                "binId": {
                    # all commented query are working
                    # "$nin": ['/^D/i', '/^R/i', '/^Trial/i']
                    # "$nin": [re.compile(r"^D(?i)")]
                    "$nin": [re.compile(r"^D(?i)"), re.compile(r"^R(?i)"), re.compile(r"^Trial(?i)")]
                    # "$regex": "^(?!D)"
                    # "$not":{"$regex": "^D","$options":"i"}
                },
                 "warehouseId": 7

            }
        },
        {
            "$group": {
                "_id": {
                    # "binId": '$binId',
                    "warehouseId": '$warehouseId',
                    "buymore_sku": '$buymore_sku'
                },
                "total": {
                    "$sum": '$quantity'
                }
            }
        }
    ]
    #products_query = "select master_masterproduct.product_id,master_masterproduct.buymore_sku from master_masterproduct right join snapdealp_snapdealpproducts on master_masterproduct.product_id = snapdealp_snapdealpproducts.product_id"
    ##products_query = "SELECT product_id, buymore_sku from master_masterproduct"
    ##for hard code testing
    products_query = "SELECT product_id, buymore_sku from master_masterproduct where product_id = 234893"
    cur_products.execute(products_query)
    products = cur_products.fetchall()
    print('Get master products product_id and buymore_sku')
    products_data = {product[1]: product[0] for product in products}
    binreco = db.api_binreco.aggregate(pipeline)

    # Fetch placed orders count for the product and warehouse
    print('Fetch orders of dispatch date greater than and equal to today')
    order_query = "Select product_id, warehouse_id, sum(qty) from api_neworder no inner join api_dispatchdetails dd " \
                  "on no.dd_id = dd.dd_id_id where date(dispatch_by_date) >= CURRENT_DATE and ((dd.fulfillment_model " \
                  "in ('merchant', '') and picklist_id = 0) or (dd.fulfillment_model = 'portal'))  and product_id != " \
                  "0 GROUP BY product_id, warehouse_id "
    cur_orders.execute(order_query)
    new_stocks = cur_orders.fetchall()

    # Store the counts of the orders
    print('Read the orders for the master stock')
    stock_orders = {str(order[0]) + '#' + str(order[1]): order[2] for order in new_stocks}

    count = 0

    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

    # Add variables to handle the stock information for portals




    # Todo: Add New Portal stock variable here

    # Loop through stocks and prepare inventory for portal and master stock
    print('loop through the wms stock')

    for item  in binreco:

        buymore_sku = item['_id']['buymore_sku'].strip()

        if buymore_sku in ('', None):
            continue
        try:
            buymore_sku = buymore_sku.replace('\xa0', ' ')
            product_id = products_data[buymore_sku]
        except KeyError as e:
            print(buymore_sku)
            print('key error')
            continue
        buymore_sku = buymore_sku.replace("'", "''")

        warehouse_id = item['_id']['warehouseId']
        # print("warehouse_id",warehouse_id)
        stock = int(item['total'])
        key = str(product_id) + '#' + str(warehouse_id)
        if key in stock_orders:
            orders = -1 * abs(stock_orders[key])
        else:
            orders = 0

        if stock < 0:
            stock = 0
        print("product_id===", product_id)
        print("warehouse_id===", warehouse_id)
        print('Check if stock data already exists')
        ##added for testing

        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/orders')
        # query = "SELECT * from api_masterstock where product_id = " + str(product_id) + " and warehouse=" + str(
        #     warehouse_id)
        query = "SELECT * from api_masterstock where product_id = " + str(product_id) + " and warehouse= 7"
        master_stock_df = pd.read_sql(query, engine)
        engine.dispose()
        if master_stock_df.empty:
            master_stock = True
        else:
            master_stock = master_stock_df

        # print("master_stock===>", master_stock)

        # cur_orders.execute(
        #     "SELECT * from api_masterstock where product_id=" + str(product_id) + " and warehouse=" + str(warehouse_id))
        # master_stock2 = cur_orders.fetchone()
        # print("master_stock2", master_stock2)
        # print("master_stock",master_stock)
        print("stock====",stock)
        print("orders====", orders)
        quantity = stock + orders
        if quantity < 0:
            quantity = 0

        # snapdeal stock
        snapdeal_product = get_snapdeal_product(cur_products, product_id)
        print("snapdeal_product--289",snapdeal_product)
        if snapdeal_product is None:
            continue
        if snapdeal_product['unique_id'] not in snapdeal_current_stock:
            account = snapdeal_account_data[snapdeal_product['account_id']]
            # print("account===294>",account)
            warehouses = account['warehouses']
            # print("warehouses===296>", warehouses)
            ##hardcoded for portal_id 20
            xsellerauthztoken = "9381cf6a-b03e-4ba9-a098-3584e124439b"
            ##commented code
            # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
            snapdeal_stock = get_snapdeal_stock(snapdeal_product['unique_id'], account, xsellerauthztoken)
            # print("snapdeal_stock===300>",snapdeal_stock)
            if str(snapdeal_product['unique_id']) not in snapdeal_current_stock:
                snapdeal_current_stock[str(snapdeal_product['unique_id'])] = {}
                print("snapdeal_current_stock--303->", snapdeal_current_stock)
            try:
                if 'message' not in snapdeal_stock and snapdeal_stock['payload']['live']:
                    snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = \
                        snapdeal_stock['payload']['availableInventory']
                    print("snapdeal_current_stock--308->", snapdeal_current_stock)
                else:
                    continue
            except:
                print("some error occured")
                continue
        # print("unoueid snapdeal",snapdeal_product['unique_id'])
        # print("unoueid snapdeal", snapdeal_product['xsellerauthztoken'])
        print("quantity===",quantity)
        print("snapdeal_current_stock--->",snapdeal_current_stock)
        ##testing
        xsellerauthztoken = '9381cf6a-b03e-4ba9-a098-3584e124439b'
        # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
        print("xsellerauthztoken>>>>>",xsellerauthztoken)
        print("snapdeal_current_stock====>><<<",snapdeal_current_stock)
        # test=snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]
        # print("test====>",test)
        # if quantity == snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]:
        #     continue

        snapdeal_quantity = quantity - 5
        if snapdeal_quantity < 0:
            snapdeal_quantity = 0
        if snapdeal_quantity > 50:
            snapdeal_quantity = 50
        # print("snapdeal_quantity=====>>>>><<<",snapdeal_quantity)
        snapdeal_csv_row = {
            'supc': str(snapdeal_product['unique_id']),
            'availableInventory': snapdeal_quantity
        }

        if xsellerauthztoken in snapdeal_set:
            snapdeal_set[xsellerauthztoken]['data'].append(snapdeal_csv_row)
        else:
            snapdeal_set[xsellerauthztoken] = {
                'account': account,
                'data': [
                    snapdeal_csv_row
                ]
            }
        if len(snapdeal_set[xsellerauthztoken]['data']) == 50:
            res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                        snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
            if 'message' in res['metadata']:
                message = res['metadata']['message']
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        'status': 'Failed'
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                message = res['payload']['uploadId']
                uploadids.append(message)
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    if message not in snapdeal_qty:
                        snapdeal_qty[message] = {}
                    snapdeal_qty[message][item['supc']] = item['availableInventory']

            snapdeal_set[xsellerauthztoken]['data'] = []

        # Todo: Add new Portal update inventory code here

        #     #master stock update code
        master_row = {
            'buymore_sku': buymore_sku,
            'product_id': product_id,
            'warehouse': warehouse_id,
            'stock': quantity,
            'status': False,
            'error': ''
        }
        print("master_stock===>===>", master_stock)
        if master_stock is True:
            print('Insert New Record')
            ####to resolve duplication issue in postgresql
            engine = create_engine(
                'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/orders')
            query0 = "select masterstock_id from api_masterstock ORDER BY masterstock_id DESC limit 1"
            last_fr_id = pd.read_sql(query0, engine)
            last_fr_id.to_csv('/tmp/z1.csv', index=False)
            ccc = int(last_fr_id['masterstock_id'])
            # print("ccc", ccc)
            query2 = "select setval('api_masterstock_masterstock_id_seq', " + str(ccc) + " , true)"
            # print("query2=", query2)
            a = pd.read_sql(query2, engine)
            # print("a", a)
            ####end to resolve duplication issue in postgresql

            try:
                cur_orders.execute(
                    "Insert into api_masterstock (buymore_sku, product_id, warehouse, stock, orders, lost, status, "
                    "updated_time) VALUES ('" + buymore_sku + "', " + str(
                        product_id) + ", " + str(warehouse_id) + ", " + str(quantity) + ", 0, 0, false, NOW())")
                conn_orders.commit()
            except:
                master_row['error'] = sys.exc_info()[0]
        else:
            # print('Update existing Record')
            try:
                cur_orders.execute("Update api_masterstock set stock = " + str(
                    quantity) + ", orders = 0, lost = 0, status = False, updated_time = NOW() where product_id= " + str(
                    product_id) + " and warehouse = " + str(warehouse_id))
                conn_orders.commit()
            except:
                master_row['error'] = sys.exc_info()[0]
        master_writer.writerow(master_row)
        count += 1
    master_update_csv.close()

    # Check for the pending inventory data to be updated for all portals

    # Update flipkart Pending inventory data
    # print('check for the pending records')


    # snapdeal pending inventory set
    if len(snapdeal_set):
        for xsellerauthztoken in snapdeal_set:
            if len(snapdeal_set[xsellerauthztoken]['data']) > 0:
                res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                            snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
                if 'message' in res['metadata']:
                    message = res['metadata']['message']
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        snapdeal_csv_row = {
                            'product_id': item['supc'],
                            'quantity': item['availableInventory'],
                            'message': message,
                            'status': 'Failed'
                        }
                        snapdeal_writer.writerow(snapdeal_csv_row)
                else:
                    message = res['payload']['uploadId']
                    uploadids.append(message)
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        if message not in snapdeal_qty:
                            snapdeal_qty[message] = {}
                        snapdeal_qty[message][item['supc']] = item['availableInventory']
    #
    # # Write snapdeal file with uploadids status
    for message in uploadids:
        page = 0
        total_pages = 1
        while page < total_pages:
            # res = verify_stock_update(account, xsellerauthztoken, message, page)
            res = verify_stock_update(account, xsellerauthztoken, message, page +1)
            page = page + 1
            if 'message' in res['metadata']:
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    status =res['metadata']['message']
                    if status == " Update request is under process, Please try after 3 minutes" :
                        final_status = 'success'
                    else:
                        final_status = res['metadata']['message']
                    print("final_status==",final_status)
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        # 'status': res['metadata']['message']
                        'status': final_status
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                results = res['payload']['supcResults']
                upload_id = res['payload']['uploadId']
                page = res['payload']['currentPage']
                total_pages = res['payload']['totalPages']


                for result in results:
                    # print("result===<<",result)
                    try:
                        quantity1 = snapdeal_current_stock[result['supc']][xsellerauthztoken]
                        # quantity=snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = snapdeal_stock['payload']['availableInventory']
                        print("quantity--try==", quantity1)
                    except:
                        quantity1 = 0
                        print("quantity--except==", quantity1)
                    print("final_quantity", quantity1)
                    q = quantity1
                    print("snapdeal_quantity===inner==", snapdeal_quantity)
                    print("result===", result)
                    snapdeal_csv_row = {
                        'product_id': result['supc'],
                        # 'quantity': snapdeal_quantity[upload_id][result['supc']],
                        'quantity': q,
                        # 'quantity': snapdeal_quantity,
                        'message': upload_id,
                        'status': result['state']
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
    snapdeal_update_csv.close()

    # Todo: Add New portal Pending inventory update code here

    # upload the log files to server
    print('upload file to dropbox')
    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)

    # master stock refresh file
    with open(master_filefrom, 'rb') as f:
        dbx.files_upload(f.read(), master_fileto, mode=dropbox.files.WriteMode.overwrite)

    # snapdeal stock refresh file
    with open(snapdeal_filefrom, 'rb') as f:
        dbx.files_upload(f.read(), snapdeal_fileto, mode=dropbox.files.WriteMode.overwrite)

    # Todo: Add new portal refresh file upload code here

    # Close DB Connections
    conn_products.close()
    conn_orders.close()

    print('Total: ' + str(count))

    return {
        'statusCode': 200,
        'body': json.dumps('The data is generated')
    }


main()
