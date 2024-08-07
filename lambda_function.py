def setup_environment():
    # Setup object representing contents of a DynamoDB table
    table_contents = {
        "tb_assets_new": {
            "Items": [
                {
                    "Item": {
                        "new_aws_account_id": "111111111111",
                        "new_aws_account_env": "prod",
                        "control_account_id": "999999999999",
                        "control_cdp_account_id": "989898989898"
                    }
                },
                {
                    "Item": {
                        "new_aws_account_id": "333333333333",
                        "new_aws_account_env": "prod"
                    }
                },
                {
                    "Item": {
                        "new_aws_account_id": "444444444444",
                        "new_aws_account_env": "hom",
                        "control_cdp_account_id": "989898989898"
                    }
                }
            ]
        },
        "tb_assets_old": {
            "Items": [
                {
                    "Item": {
                        "old_aws_account_id": "111111111111",
                        "old_aws_account_env": "prod"
                    }
                },
                {
                    "Item": {
                        "old_aws_account_id": "222222222222",
                        "old_aws_account_env": "dev"
                    }
                },
                {
                    "Item": {
                        "old_aws_account_id": "333333333333",
                        "old_aws_account_env": "prod"
                    }
                },
                {
                    "Item": {
                        "old_aws_account_id": "444444444444",
                        "old_aws_account_env": "hom"
                    }
                }
            ]
        },
        "tb_ddb_new": {
            "Items": [
                {
                    "Item": {
                        "new_aws_account_id": "111111111111",
                        "new_database_name": "db_source_teste",
                        "new_aws_account_env": "prod",
                        "control_account_id": "999999999999"
                    }
                }
            ]
        },
        "tb_ddb_old": {
            "Items": [
                {
                    "Item": {
                        "old_aws_account_id": "111111111111",
                        "old_database_name": "db_source_teste",
                        "old_aws_account_env": "prod"
                    }
                },
                {
                    "Item": {
                        "old_aws_account_id": "222222222222",
                        "old_database_name": "db_compartilhado_teste",
                        "old_aws_account_env": "dev"
                    }
                }
            ]
        }
    }

    return table_contents


# TODO: refactor docstring and code considering the new variable
# glue_database_type as a parameter and return value
def define_dynamodb_table_properties(database_name):
    possible_matches = ["db_source", "db_compartilhado"]

    check_for_mesh_database = any(
        match in database_name for match in possible_matches
    )

    if not check_for_mesh_database:
        glue_database_type = "CDP"
        dynamodb_table_choice_object = {
            "default_table_properties": {
                "table_name": "tb_assets_new",
                "partition_key": "new_aws_account_id",
                "environment_key": "new_aws_account_env",
                "control_account_id_key": "control_account_id",
                "control_cdp_account_id_key": "control_cdp_account_id",
                "glue_database_type": glue_database_type
            },
            "legacy_table_properties": {
                "table_name": "tb_assets_old",
                "partition_key": "old_aws_account_id",
                "environment_key": "old_aws_account_env",
                "glue_database_type": glue_database_type
            },
        }
    else:
        glue_database_type = "MESH"
        dynamodb_table_choice_object = {
            "default_table_properties": {
                "table_name": "tb_ddb_new",
                "partition_key": "new_aws_account_id",
                "database_name_key": "new_database_name",
                "environment_key": "new_aws_account_env",
                "control_account_id_key": "control_account_id",
                "glue_database_type": glue_database_type
            },
            "legacy_table_properties": {
                "table_name": "tb_ddb_old",
                "partition_key": "old_aws_account_id",
                "database_name_key": "old_database_name",
                "environment_key": "old_aws_account_env",
                "glue_database_type": glue_database_type
            }
        }

    return glue_database_type, dynamodb_table_choice_object


def get_item(
    table_contents,
    table_name,
    partition_key,
    account_id
):
    response = {
        "Item": None,
        "Error": ""
    }

    for item in table_contents[table_name]["Items"]:
        if account_id == item["Item"][partition_key]:
            response["Item"] = item["Item"]
            break

    if not response["Item"]:
        response["Error"] = "No item was found"

    return response


# TODO: Move to utils.py and refactor the files where it is used
def default_control_account_id(environment):
    """
    Defines the default control account ID from the legacy control account
    based on the environment

    Attributes:
    - ``environment`` (str): Environment to be used

    Returns:
    - ``control_account_id`` (str): the legacy control account ID to be used
    """
    account_ids = {
        "dev": "default_dev",
        "hom": "default_hom",
        "prod": "default_prod"
    }
    return account_ids[environment]


# TODO: Move to utils.py
def define_control_account_id(
    item: dict,
    table_properties: dict
):
    """
    Defines the control account ID from legacy or new control account
    based on the item retrieved from the DynamoDB table

    Attributes:
    - ``item`` (dict): Item retrieved from the DynamoDB table
    - ``table_properties`` (dict): Table properties object

    Returns:
    - ``control_account_id`` (str): the control account ID to be used
    """
    control_account_id_key = (
        "control_cdp_account_id"
        if table_properties["glue_database_type"] == "CDP"
        else "control_account_id"
    )

    if control_account_id_key in item:
        control_account_id = item[control_account_id_key]
    else:
        control_account_id = default_control_account_id(
            environment=item[table_properties["environment_key"]]
        )

    if not control_account_id:
        raise Exception("Lambda was not able to define the control account ID")

    return control_account_id


def retrieve_control_account_id_from_item(
    table_contents,
    dynamodb_table_choice_object: dict,
    account_id: str
):
    control_account_id = None

    for key in dynamodb_table_choice_object.keys():

        # TODO: Verify how this method is implemented and if it is necessary
        # to log the table being queried
        print(f"Querying {repr(dynamodb_table_choice_object[key]['table_name'])} table.")

        query_item_response = get_item(
            table_contents,
            dynamodb_table_choice_object[key]["table_name"],
            dynamodb_table_choice_object[key]["partition_key"],
            account_id
        )

        if (
            query_item_response["Error"] == "No item was found"
            and key == "default_table_properties"
        ):
            print(
                f"No item was found in the default table. Proceeding to query the legacy table."
            )
            continue

        if (
            query_item_response["Item"] is None
            and key == "legacy_table_properties"
        ):
            print(
                f"No item was found in the legacy table."
            )
            raise Exception(
                "Account ID is not a valid producer or consumer account"
            )

        print(f"Item retrieved: {repr(query_item_response['Item'])}")
        print(f"Defining control account ID.")
        control_account_id = define_control_account_id(
            item=query_item_response["Item"],
            table_properties=dynamodb_table_choice_object[key]
        )

        print(f"Retrieved control account ID: {repr(control_account_id)}")

        return control_account_id


def main():
    table_contents = setup_environment()

    # # Case 1: Database is a MESH database and control_account_id is present
    # database_name = "db_source_teste"
    # account_id = "111111111111"

    # dynamodb_table_choice_object = define_dynamodb_table_properties(
    #     database_name
    # )
    # control_account_id = retrieve_control_account_id_from_item(
    #     table_contents,
    #     dynamodb_table_choice_object,
    #     account_id
    # )

    # print(control_account_id)

    # # Case 2: Database is a MESH database and control_account_id is not present
    # database_name = "db_compartilhado_teste"
    # account_id = "222222222222"

    # dynamodb_table_choice_object = define_dynamodb_table_properties(
    #     database_name
    # )
    # control_account_id = retrieve_control_account_id_from_item(
    #     table_contents,
    #     dynamodb_table_choice_object,
    #     account_id
    # )

    # print(control_account_id)

    # # Case 3: Database is a CDP database and control_cdp_account_id is present
    # database_name = "rt2"
    # account_id = "444444444444"

    # dynamodb_table_choice_object = define_dynamodb_table_properties(
    #     database_name
    # )
    # control_account_id = retrieve_control_account_id_from_item(
    #     table_contents,
    #     dynamodb_table_choice_object,
    #     account_id
    # )

    # print(control_account_id)

    # # Case 4: Database is a CDP database and control_cdp_account_id is not
    # # present
    # database_name = "rt2"
    # account_id = "333333333333"

    # dynamodb_table_choice_object = define_dynamodb_table_properties(
    #     database_name
    # )
    # control_account_id = retrieve_control_account_id_from_item(
    #     table_contents,
    #     dynamodb_table_choice_object,
    #     account_id
    # )

    # print(control_account_id)

    database_name = "db_source_teste"
    account_id = "111111111111"

    print(f"User account ID: {repr(account_id)}")
    print(f"Requested database name: {repr(database_name)}")

    glue_database_type, dynamodb_table_choice_object = define_dynamodb_table_properties(
        database_name
    )

    print(f"Database type: {repr(glue_database_type)}")

    print("Retrieving control account ID.")
    control_account_id = retrieve_control_account_id_from_item(
        table_contents,
        dynamodb_table_choice_object,
        account_id
    )

    return control_account_id


if __name__ == "__main__":
    control_account_id = main()
