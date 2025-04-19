
## Column Mapping

### Car_Brand Status
source : table car_brand  (staging)

target : table car_brand

| Source Column   | Target Column   | Transformation                                   |
|----------------|----------------|----------------------------------------------------|
| uuid generated | `brand_car_id` |----------------------------------------------------|
| `brand_car_id` | `brand_car_id_nk` |----------------------------------------------------|
| `brand_name`        | `brand_name`        |----------------------------------------------------|




### us_state  Status
source : table us_state (staging)

target : table us_state
| Source Column   | Target Column   | Transformation |
|----------------|----------------|---------------|
| uuid generated  | `id_state`   | - |
| `id_state`        | `id_state_nk`        | - |
| `code`   | `code`   | - |
| `name`   | `name`   | - |


### Car_sales Table
source : table car_sales (Staging), Car_brand (DWH), US_State (DWH)

target : table car_sales
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|-----------------------------|----------------------------------------------------|
| uuid generated             | `id_sales`              | - |
| `id_sales`                      | `id_sales_nk`                       | - |
| `year`                      | `year`                       | convert to integer |
| `brand_car_id` (DWH),    `brand_car_id` (staging)           | `brand_car_id`                | Join  table car_sales (Staging) and Car_brand (DWH), get brand_car_id from car_brand (DWH) table|
| `transmisson`             | `transmission`              | - |
| `"id_state"`    (DWH) , `id_state` (staging)           | `"id_state"`                 | Join  table car_sales (Staging) and us_state (DWH), get id_state from ud_state (DWH) table |
| `condition`                  | `condition`                   |  |
| `odometer`                  | `odometer`                   | - |
| `color`                     | `color`                      | - |
| `interior`                  | `interior`                   | - |
| `"mmr"`                    | `"mmr"`                     | - |
| `"sellingprice"`                  | `"selling_price"`                   | - |

