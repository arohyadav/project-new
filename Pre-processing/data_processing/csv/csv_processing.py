import apache_beam as beam
import csv
import io
import json
import os
import psycopg2
import logging
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import WriteToPubSub

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../dataplatform.json"

column_mapping = {'Customer_ID': 'CustomerID', 'Restaurant_ID': 'RestaurantID', 'Customer_FirstName': 'CustomerGivenName', 'Customer_MiddleName': 'CustomerMiddleName', 'Customer_LastName': 'CustomerSurname', 'Customer_Title': 'CustomerTitle', 'Customer_Age': 'CustomerAge', 'Customer_Gender': 'CustomerGender', 'Customer_Email': 'CustomerEmail', 'Customer_Address': 'CustomerAddress', 'Customer_Location': 'CustomerLocation', 'Resturant_City_ID': 'ResturantCityID', 'Customer_DOB': 'CustomerBirthDate', 'Customer_Nationality': 'CustomerNationality', 'Customer_Anniversary': 'CustomerAnniversary', 'Customer_Registration_Date': 'CustomerRegistrationDate', 'Customer_IncomeLevel': 'CustomerIncomeLevel', 'Customer_Dietary_Restrictions': 'CustomerDietaryRestrictions', 'Customer_PhoneNo': 'CustomerPhoneNo', 'Customer_Allergies': 'CustomerAllergies', 'Customer_Time_Joined': 'CustomerTimeJoined', 'Payment_ID': 'PaymentID', 'Order_ID': 'OrderID', 'Payment_Date': 'PaymentDate', 'Payment_time': 'Paymenttime', 'Amount_Paid': 'AmountPaid', 'Transaction_ID': 'TransactionID', 'Promotion_Type': 'PromotionType', 'Discount': 'Discount', 'Total_Amount': 'TotalAmount', 'Payment_Method': 'PaymentMethod', 'Bill_ID': 'BillID', 'Bill_Date': 'BillDate', 'Paymeny_status': 'PaymentStatus', 'Tips': 'Tips', 'Tax': 'Tax', 'Notes': 'Notes', 'Return_ID':
'ReturnID', 'Return_Date': 'ReturnDate', 'Return_Time': 'ReturnTime', 'Refund_ID': 'RefundID', 'Refund_Amount': 'RefundAmount', 'Refund_Date': 'RefundDate', 'Refund_time': 'Refundtime', 'LastVist_Date': 'LastVisitDate', 'LastVisit_Time': 'LastVisitTime', 'LoyaltyPoints_Used': 'LoyaltyPointsUsed', 'LoyaltyPoints_Earned': 'LoyaltyPointsEarned', 'Rewards': 'Rewards', 'LoyaltyPoints_MemberStatus': 'LoyaltyPointsMemberStatus', 'Loyalty_Tier': 'LoyaltyTier', 'Join_Date': 'JoinDate', 'Expiry_Date': 'ExpiryDate', 'IsActive': 'IsActive', 'Feedback_ID': 'FeedbackID', 'Comment_Text': 'CommentText', 'Rating': 'Rating', 'Menu_Item_Name': 'MenuItemName', 'Comment_Date': 'CommentDate', 'Comment_Time': 'CommentTime', 'Survey_Question': 'SurveyQuestion', 'Survey_date': 'Surveydate', 'Survey_Time':
'SurveyTime', 'Servey_result': 'SurveyResult', 'Recommendation': 'Recommendation', 'Customertype': 'Customertype', 'SecondaryCountryCode': 'SecondaryCountryCode', 'SecondaryContactType': 'SecondaryContactType', 'SecondaryPhoneNumber': 'SecondaryPhoneNumber', 'EmailType': 'EmailType', 'SecondaryEmail': 'SecondaryEmail', 'SecondaryEmailType': 'SecondaryEmailType', 'AddressType': 'AddressType', 'AddressLine1': 'AddressLine1', 'AddressLine2': 'AddressLine2', 'AddressLine3': 'AddressLine3', 'CityName': 'CityName', 'PostalCode': 'PostalCode', 'SecondaryAddressType': 'SecondaryAddressType', 'SecondaryAddressLine1': 'SecondaryAddressLine1', 'SecondaryAddressLine2': 'SecondaryAddressLine2', 'SecondaryAddressLine3': 'SecondaryAddressLine3', 'SecondaryCityName': 'SecondaryCityName', 'SecondaryStateProv': 'SecondaryStateProv', 'SecondaryCountryName': 'SecondaryCountryName', 'SecondaryPostalCode': 'SecondaryPostalCode', 'Latitude': 'Latitude', 'Longitude': 'Longitude', 'SecondaryLatitude': 'SecondaryLatitude', 'SecondaryLongitude': 'SecondaryLongitude', 'Occupation': 'Occupation', 'MaritalStatus': 'MaritalStatus', 'PreferredLanguage': 'PreferredLanguage', 'PrimaryCreditCard': 'PrimaryCreditCard', 'PrimaryExpiryMonth': 'PrimaryExpiryMonth', 'PrimaryExpiryYear': 'PrimaryExpiryYear', 'SecondaryCreditCard': 'SecondaryCreditCard', 'SecondaryExpiryMonth': 'SecondaryExpiryMonth', 'SecondaryExpiryYear': 'SecondaryExpiryYear', 'GovernmentIdentityProof': 'GovernmentIdentityProof', 'GovernmentIdentityExpiryDate': 'GovernmentIdentityExpiryDate', 'GovernmentIdentityNumber': 'GovernmentIdentityNumber', 'SpecialEvent': 'SpecialEvent', 'SpecialEventDate': 'SpecialEventDate', 'CustomerSince': 'CustomerSince', 'Customer_Acquistion_ID': 'CustomerAcquistionID', 'Engagement_ID': 'EngagementID', 'Restaurant_Location': 'RestaurantLocation', 'Customer_Acquired': 'CustomerAcquired', 'Channel_Costs': 'ChannelCosts', 'Conversion_Rate': 'ConversationRate', 'Budget': 'Budget', 'Acquistion_Date': 'AcquistionDate', 'Email_Marketing_ID': 'EmailMarketingID', 'Email_Sents': 'EmailSents', 'Email_Date': 'EmailDate', 'Email_Opens': 'EmailOpens', 'Email_Clicks': 'EmailClicks', 'Email_Compaign_Cost': 'EmailCompaignCost', 'ROI': 'ROI', 'Revenue_Generated':
'RevenueGenerated', 'Post_ID': 'PostID', 'Post_Date': 'PostDate', 'Post_Time': 'PostTime', 'Platform': 'Platform', 'Likes': 'Likes', 'Shares': 'Shares', 'Comments': 'Comments', 'Views': 'Views', 'Follower_Growth': 'FollowerGrowth', 'Budget_ID': 'BudgetID', 'Budget_Month': 'BudgetMonth', 'Budget_Year': 'BudgetYear', 'Total_Budget': 'TotalBudget', 'Restaurant_Expense_ID': 'RestaurantExpenseID',
'Chain_Code': 'ChainCode', 'Expense_Category': 'ExpenseCategory', 'Expense_Date': 'ExpenseDate', 'Expense_Description': 'ExpenseDescription', 'Expense_Amount': 'ExpenseAmount', 'Revenue_ID': 'RevenueID', 'Vendor_Payment_ID': 'VendorPaymentID', 'Revenue_Date': 'RevenueDate', 'Revenue_Time': 'RevenueTime', 'Tax_Amount': 'TaxAmount', 'Other_Income': 'OtherIncome', 'Total_Sales': 'TotalSales', 'Discounts': 'Discounts', 'Expenses': 'Expenses', 'Total_Revenue': 'TotalRevenue', 'Order_Type': 'OrderType', 'Order_Time': 'OrderTime', 'Order_Date': 'OrderDate', 'Order_Modifier': 'OrderModifier', 'Order_Category': 'OrderCategory', 'Order_Quantity': 'OrderQuantity', 'Total_No_Order': 'TotalNoOrder', 'Review_Comment': 'ReviewComment', 'Order_Item_ID': 'OrderItemID', 'Menu_Item_ID': 'MenuItemID', 'Modifier_Name': 'ModifierName', 'Modifier_Price': 'ModifierPrice', 'Item_Quantity': 'ItemQuantity', 'Item_Price': 'ItemPrice', 'Total_Price': 'TotalPrice', 'Order_Status': 'OrderStatus', 'Dinning_Table_ID': 'DinningTableID', 'Serve_Starttime': 'ServeStarttime',
'Server_EndTime': 'ServeEndTime', 'DurationTime': 'DurationTime', 'Inventory_ID': 'InventoryID', 'Ingredient_ID': 'IngredientID', 'Inventory_Item_Name': 'InventoryItemName', 'Inventory_Cost': 'InventoryCost', 'Inventory_Purchase_Date': 'InventoryPurchaseDate', 'Inventory_Expiration_Date': 'InventoryExpirationDate', 'Inventory_Threshold': 'InventoryThreshold', 'Inventory_Reorder_Quantity': 'InventoryReorderQuantity', 'Inventory_Unit_of_Measurement': 'InventoryUnitofMeasurement', 'Inventory_Location': 'InventoryLocation', 'Inventory_Category': 'InventoryCategory', 'Inventory_QTY_Instock': 'InventoryQTYInstock', 'Ingredient_Supplier_ID': 'IngredientSupplierID', 'Ingredient_Order_History_ID': 'IngredientOrderHistoryID', 'Ingredient_Name': 'IngredientName', 'Ingredient_Cost': 'IngredientCost', 'Ingredient_Purchase_Date': 'IngredientPurchaseDate', 'Ingredient_Purchase_Price': 'IngredientPurchasePrice', 'Ingredient_Threshold': 'IngredientThreshold', 'Ingredient_Reorder_Quantity': 'IngredientReorderQuantity', 'Ingredient_Location': 'IngredientLocation', 'Ingredient_Category': 'IngredientCategory', 'Ingredient_Last_Restocked_Date': 'IngredientLastRestockedDate', 'Ingredient_Expiration_Date': 'IngredientExpirationDate', 'Ingredient_Quantity_Instock': 'IngredientQuantityInstock', 'Recipe_ID': 'RecipeID', 'Item_ID': 'ItemID', 'Meal_Quality': 'MealQuality', 'Serving_Size': 'ServingSize', 'Meal_ID': 'MealID', 'Meals_Name': 'MealsName', 'Description': 'Description', 'Meal_Price': 'MealPrice', 'Supplier_ID': 'SupplierID', 'Supplier_Name': 'SupplierName', 'Supplier_Contact': 'SupplierContact', 'Supplier_Address': 'SupplierAddress', 'Shipping_ID': 'ShippingID', 'Suppling_Item_ID': 'SupplingItemID', 'Quantity_on_hand': 'Quantityonhand', 'Supply_Name': 'SupplyName', 'Vendor_ID': 'VendorID', 'Vendor_Name': 'VendorName', 'Vendor_Contact_Email': 'VendorContactEmail', 'Vendor_Contact_Phone': 'VendorContactPhone', 'Vendor_Address': 'VendorAddress', 'Payment_Time': 'PaymentTime', 'Payment_Amount': 'PaymentAmount', 'Vendor_Payment_Method': 'VendorPaymentMethod', 'Vendor_Payment_Delay_ID': 'VendorPaymentDelayID', 'Delay_Date': 'DelayDate', 'Delay_Time': 'DelayTime', 'Delay_Amount': 'DelayAmount', 'Delay_Reason': 'DelayReason', 'Payment_Delay': 'PaymentDelay', 'Menu_ID': 'MenuID', 'Menu_Item_Name': 'MenuItemName', 'Menu_Item_Category': 'MenuItemCategory', 'Menu_VeganFriendly': 'MenuVeganFriendly', 'Menu_Item_Description': 'MenuItemDescription', 'Menu_Item_Price': 'MenuItemPrice', 'Menu_Ingredients': 'MenuIngredients', 'Menu_Item_Ingredients_Cost': 'MenuItemIngredientsCost', 'Menu_Profit_ID': 'MenuProfitID', 'Menu_Item_Cost_Price': 'MenuItemCostPrice', 'Menu_Item_Selling_Price': 'MenuItemSellingPrice', 'Profit_Margin': 'ProfitMargin', 'Restaurant_Name': 'RestaurantName', 'Restaurant_Type': 'RestaurantType', 'Restaurant_Address': 'RestaurantAddress', 'Restaurant_StateProv': 'RestaurantStateProv', 'Restaurant_PhoneNo': 'RestaurantPhoneNo', 'Restaurant_Email': 'RestaurantEmail', 'Restaurant_Rating': 'RestaurantRating', 'Restaurant_Country': 'RestaurantCountry', 'Restaurant_PostalCode': 'RestaurantPostalCode', 'Restaurant_Website': 'RestaurantWebsite', 'Restaurant_Owner': 'RestaurantOwner', 'Brand_Code': 'BrandCode', 'Restaurant_City_ID': 'RestaurantCityID', 'Staff_ID': 'StaffID', 'Staff_Role': 'StaffRole', 'Staff_FirstName': 'StaffFirstName', 'Staff_MiddleName': 'StaffMiddleName', 'Staff_LastName': 'StaffLastName', 'Staff_Position': 'StaffPosition', 'Staff_Title': 'StaffTitle', 'Staff_Email': 'StaffEmail', 'Staff_Phone_Number': 'StaffPhoneNumber', 'Staff_DateofBirth': 'StaffDateofBirth', 'Staff_Address': 'StaffAddress', 'Hire_Date': 'HireDate', 'Staff_Salary':
'StaffSalary', 'Labor_ID': 'LaborID', 'Working_Month': 'WorkingMonth', 'Shift_StartTime': 'ShiftStartTime', 'Shift_EndTime': 'ShiftEndTime', 'Hour_Worked': 'HourWorked', 'Hourly_Wage': 'HourlyWage', 'Overtime_Hours': 'OvertimeHours', 'Monthly_Salary': 'MonthlySalary', 'Tips_Received': 'TipsReceived'}

# def fetch_column_mapping(host, port, database, user, password):
#     column_mapping = {}
#     try:
#         conn = psycopg2.connect(
#             host=host,
#             port=port,
#             database=database,
#             user=user,
#             password=password
#         )
#         print("Successfully connected to the PostgreSQL database")
#     except psycopg2.Error as e:
#         print("Failed to connect to the PostgreSQL database:", e)
#         return column_mapping

#     if conn is not None and not conn.closed:
#         try:
#             cursor = conn.cursor()

#             # List of child table names
#             child_tables = ["child_customers", "child_marketing", "child_finance"]

#             for child_table in child_tables:
#                 # Fetch data from the current child table
#                 cursor.execute(f'SELECT field_id, field_names FROM "RestaurantMVP".{child_table}')
#                 child_data = cursor.fetchall()

#                 # Fetch data from the master table for each child table
#                 cursor.execute('SELECT field_id, field_names FROM "RestaurantMVP".master_restaurant')
#                 master_data = cursor.fetchall()

#                 # Perform inner join to match field_id from the current child table with master table
#                 joined_data = {c[0]: (c[1], m[1]) for c in child_data for m in master_data if c[0] == m[0]}

#                 # Store the field_names from both tables in a dictionary
#                 for field_id, (child_field_name, master_field_name) in joined_data.items():
#                     column_mapping[child_field_name] = master_field_name

#             print("Column mapping fetched successfully")
#         except Exception as e:
#             print("Failed to fetch column mapping:", e)
#         finally:
#             cursor.close()
#             conn.close()
#     else:
#         print("Connection to the PostgreSQL database is not established.")

#     return column_mapping

# # Example usage:
# host = "34.29.183.25"
# port = "5432"
# database = "postgres"
# user = "postgres"
# password = "FURc+($fTmqAgXF2"

# column_mapping = fetch_column_mapping(host, port, database, user, password)

def clean_column_name(column_name):
    column_name = column_name.replace('\n', ' ').strip()
    while '(' in column_name and ')' in column_name:
        start_idx = column_name.index('(')
        end_idx = column_name.index(')')
        column_name = column_name[:start_idx] + column_name[end_idx + 1:]
    return column_name.strip()

class ReadCSVAsDict(beam.DoFn):
    def process(self, element, batch_size, *args, **kwargs):
        rows = csv.reader(io.TextIOWrapper(element.open(), encoding='utf-8'))
        header = next(rows)
        mapped_header = [col for col in header if clean_column_name(col) in column_mapping]

        batch = []
        for row in rows:
            extracted_row = {
                column_mapping[clean_column_name(col)]: row[idx]
                for idx, col in enumerate(header)
                if col in mapped_header
            }
            batch.append(extracted_row)

            if len(batch) == batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

class ConvertToJSON(beam.DoFn):
    def process(self, batch, *args, **kwargs):
        for row in batch:
            if row:
                yield json.dumps(row)

class ReadCSVFiles(beam.PTransform):
    def __init__(self, file_pattern, batch_size):
        self.file_pattern = file_pattern
        self.batch_size = batch_size

    def expand(self, pcoll):
        return (pcoll
                | fileio.MatchFiles(self.file_pattern)
                | fileio.ReadMatches()
                | beam.ParDo(ReadCSVAsDict(), batch_size=self.batch_size)
               )

class RemoveEmptyDicts(beam.DoFn):
    def process(self, element):
        if element:
            yield element

# Assuming you have a global `column_mapping` defined in your `main.py`

def process_csv_files(p, file_pattern, batch_size, pubsub_topic):
    csv_data = (
        p
        | f'match files {file_pattern}' >> ReadCSVFiles(file_pattern=file_pattern, batch_size=batch_size)
    )
    filtered_data = csv_data | f'RemoveEmptyDicts {file_pattern}' >> beam.ParDo(RemoveEmptyDicts())
    json_data = filtered_data | f'ConvertToJSON {file_pattern}' >> beam.ParDo(ConvertToJSON())

    encoded_json_data = json_data | f"Serialize JSON data {file_pattern}" >> beam.Map(lambda json_data: json_data.encode('utf-8'))

    encoded_json_data | f"Write to Pub/Sub {file_pattern}" >> WriteToPubSub(
        topic=pubsub_topic,
        with_attributes=False
    )
