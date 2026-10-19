# Kế hoạch chi tiết: Big Data Pipeline

Đây là bản chốt, thay cho các phần chi tiết trước đó. Nguyên tắc để không over-engineer: mỗi thành phần phải trả lời được hai câu hỏi, “nó giải quyết vấn đề cụ thể nào của pipeline này” và “bỏ nó thì cái gì hỏng”. Thành phần nào chưa có vấn đề thật thì cắt hoặc hoãn.

## 1. Chốt phạm vi

| Thành phần | Quyết định | Lý do |
| --- | --- | --- |
| Generator/replay Python (confluent-kafka) | Giữ | Dataset gốc quá nhỏ (khoảng 822 event/ngày, 130 nghìn dòng), phải sinh dữ liệu mới có Volume và Velocity |
| Kafka một broker (KRaft), một topic `events.raw`, 3 partition | Giữ | Tách nguồn khỏi xử lý, đệm tải, phát lại được, partition cho song song |
| DLQ topic, REST collector | Cắt | Bản ghi hỏng đã vào quarantine ở Spark, một cơ chế là đủ; producer gửi thẳng vào Kafka |
| Spark 3.5 bằng PySpark, standalone: 1 master + 1 worker | Giữ | Xử lý phân tán đúng nội dung môn |
| Worker thứ hai | Hoãn (compose profile riêng) | Chỉ bật khi làm thí nghiệm tùy chọn |
| Data lake = thư mục Parquet trên volume dùng chung, địa chỉ qua biến `LAKE_URI` | Giữ | Đủ cho giai đoạn Storage. MinIO/S3A chỉ nâng cấp nếu còn dư thời gian (cần thêm jar S3A và image MinIO có thể không còn tải được) |
| Delta Lake, HDFS, GraphFrames, ClickHouse, Superset, Airflow, Prometheus | Cắt | Chưa có vấn đề nào cần chúng |
| Postgres | Giữ | Grafana cần nơi truy vấn nhanh cho bảng gold nhỏ |
| Grafana (cấu hình bằng file provisioning), 2 dashboard | Giữ | KPI, bộ lọc, màn hình live |
| Rule engine | Giữ, nhưng là một script Python chạy vòng lặp, không FastAPI | Poll Postgres và gửi Telegram không cần web server |
| RFM + k-means (MLlib) | Tùy chọn, làm cuối | Cộng điểm phân tích nhưng không nằm trên đường găng |
| Makefile | Thay bằng docker compose profiles | Windows không có make mặc định; mỗi job là một service |

Quyết định đã chốt thay cho các câu hỏi mở trước đó: PySpark, Parquet thuần, Grafana, Postgres.

## 2. Kiến trúc chốt

```text
[generator] --JSON, key=user_session--> [Kafka: events.raw]
                                              |
                              [stream_app.py: 1 ứng dụng Spark, 2 query]
                               |                          |
          query 1: ghi bronze (10 giây)      query 2: cửa sổ event-time 1 phút
                               |                          |  (watermark 2 phút, upsert)
                      [lake/bronze]                [Postgres: kpi_stream_1min]
                               |
                [silver_job.py] --> [lake/silver] + [lake/quarantine] + báo cáo DQ
                               |
                [gold_job.py]   --> [lake/gold] --JDBC--> [Postgres: kpi_*]
                                                              |
                                  [Grafana: Business KPIs, Live stream]
                                  [rules_worker.py] --> [Telegram] + [action_log]
```

Hai job streaming được gộp thành một ứng dụng Spark (một driver, hai query) để tiết kiệm RAM. Job batch chạy theo yêu cầu bằng `docker compose run --rm`.

## 3. Ngân sách tài nguyên cho máy của bạn

Máy: Ryzen 5 7535HS (6 nhân, 12 luồng), RAM 15,3 GB dùng được (lúc chụp đang dùng khoảng 7,5 GB cùng trình duyệt), ổ SSD, Windows. Với cấu hình này nên giới hạn Docker bằng file `C:\Users\<tên bạn>\.wslconfig`:

```ini
[wsl2]
memory=6GB
processors=8
swap=2GB
```

Để code trong hệ thống file của WSL (ext4) thay vì `/mnt/c` để volume chạy nhanh hơn.

| Container | RAM dự kiến |  |
| --- | --- | --- |
| Kafka (heap 512 MB) | 0,8 GB |  |
| Spark master | 0,4 GB |  |
| Spark worker (4 nhân; hai ứng dụng, mỗi ứng dụng `spark.cores.max=2` và executor 1 GB) | 3,0 GB |  |
| Postgres | 0,3 GB |  |
| Grafana | 0,3 GB |  |
| Producer + rules worker | 0,3 GB |  |
| **Tổng** | **khoảng 5,1 GB** |  |

- Chạy streaming và batch cùng lúc được nhờ giới hạn nhân cho từng ứng dụng. Khi chạy thí nghiệm lớn, đóng bớt trình duyệt.
- Mục tiêu quy mô: 10 triệu event là mức chính; 25–50 triệu là tùy chọn, cần ít nhất 20 GB ổ trống. Bạn sẽ kiểm chứng giới hạn thật ở bước đầu tiên. Đặt `retention.bytes` nhỏ cho Kafka hoặc xóa topic giữa các lần chạy.

## 4. Dữ liệu và quy tắc

**Generator.** Hai chế độ: `backfill` giữ `event_time` gốc (cho xử lý lô và thí nghiệm quy mô), `live` đặt `event_time = bây giờ` (cho demo dashboard). Mỗi message có `event_id` = SHA-1 của (user\_session, event\_time, product\_id, event\_type) để phát lại không tạo bản trùng, và `producer_ts` để đo độ trễ. Phân phối lấy từ dữ liệu thật: trung bình 2,0 event/session (trung vị 1), cart/view khoảng 9,7%, purchase/cart khoảng 60,6%, sản phẩm lấy mẫu từ catalog thật (3.640 sản phẩm, 10 brand). Chèn lỗi có chủ đích theo tỉ lệ thật: category\_code thiếu 6,7%, user\_session thiếu 0,015%, trùng 0,09%, thêm JSON hỏng 0,05% và 5% event đến trễ tới 3 phút. Có hai kịch bản demo: `cart_abandon_spike` và `conversion_drop`.

| Quy mô | Tốc độ | Thời gian phát |
| --- | --- | --- |
| 129.949 (như repo cũ) | 10 event/giây | khoảng 3,6 giờ |
| 129.949 | 5.000 event/giây | khoảng 26 giây |
| 1 triệu | 5.000 event/giây | khoảng 3,3 phút |
| 10 triệu | 20.000 event/giây | khoảng 8,3 phút |

**Các lớp trong lake.**

| Lớp | Partition | Nội dung |
| --- | --- | --- |
| bronze | `ingest_date` | Dữ liệu gốc dạng chuỗi, `event_id`, `kafka_partition`, `kafka_offset`, `ingest_ts`; chỉ ghi thêm |
| silver | `event_date` | Kiểu đúng (`event_ts` UTC, id dạng long), brand chữ thường, `category_code` thiếu thì `unknown`, `category_l1`, `is_price_outlier`; ghi đè theo partition nên chạy lại không bị nhân đôi |
| gold | tùy bảng | Các bảng KPI ở mục dưới |
| quarantine | `rule_id` | Bản ghi bị loại kèm lý do |

**Quy tắc bronze → silver** (số dòng thật trên dataset gốc):

| Quy tắc | Hành động | Dòng ảnh hưởng |
| --- | --- | --- |
| R1: thiếu `user_session` | Quarantine | 20 |
| R2: trùng khóa tự nhiên | Giữ một bản | 119 |
| R3: `category_code` thiếu | Gán `unknown` | 8.645 |
| R4: giá lớn hơn P99,9 (3.717,65) | Gắn cờ, vẫn giữ | khoảng 130 |
| R5: giá nhỏ hơn hoặc bằng 0 | Quarantine | 0 |

Đối soát: 129.949 − 20 − 119 = **129.810 dòng silver**, khớp với lần chạy thử bằng PySpark 3.5.3 của mình.

**Ba điều rút ra từ lần chạy thử (hãy tự tái hiện và ghi vào báo cáo):**

1. Spark SQL không cho `SUM` trên biểu thức boolean; dùng `COUNT_IF(...)` hoặc `SUM(CASE WHEN ...)`.
2. `approxQuantile` với sai số 0,001 trả về đúng giá trị lớn nhất (42.590,13) thay vì P99,9; đặt sai số 0 mới ra 3.717,65.
3. Ghi 130 nghìn dòng với `partitionBy(event_date)` sinh ra 632 file Parquet nhỏ, tổng 13 MB so với CSV 16,6 MB. Dự kiến `repartition(event_date)` trước khi ghi sẽ gom về một file mỗi ngày; thí nghiệm X2 sẽ kiểm chứng.

**Bảng gold (bốn bảng cốt lõi cộng một bảng từ streaming):**

| Bảng | Định nghĩa |
| --- | --- |
| `kpi_daily` | Theo ngày: views, carts, purchases, doanh thu (tổng giá của purchase), purchase/view |
| `kpi_brand_funnel` | Theo brand: view→cart = carts/views; cart→purchase = purchases/carts |
| `top_products` | Top sản phẩm theo purchase; sản phẩm nhiều view ít mua |
| `abandoned_carts` | Mỗi dòng là một lần cart không có purchase cùng `product_id` trong cùng session trong 30 phút sau đó |
| `kpi_stream_1min` | Theo cửa sổ 1 phút và event\_type: số lượng, doanh thu (do `stream_app` ghi, khóa chính window\_start + event\_type) |

Kết quả mẫu trên dataset gốc để đối chiếu (3 brand đầu theo purchase): msi 19.940 view, 3.135 cart, 1.788 purchase (view→cart 15,72%, cart→purchase 57,03%); gigabyte 22.759, 3.152, 1.741 (13,85%, 55,23%); asus 24.268, 2.128, 1.261 (8,77%, 59,26%). Giỏ hàng bỏ dở: 6.397 trên 10.924 lần cart (khoảng 58,6%); nhiều nhất là gigabyte 1.939 và msi 1.880.

**Hạn chế dữ liệu cần ghi trong báo cáo:** 627 session có purchase mà không có cart (theo dõi giỏ hàng không đầy đủ, nên tỉ lệ bỏ giỏ là ước lượng); 23 session gắn với nhiều hơn một user; vì trung vị session chỉ có 1 event, funnel theo session ít ý nghĩa, nên dùng funnel theo event và chỉ dùng mức session cho giỏ hàng bỏ dở.

## 5. Streaming, dashboard và hành động

- **stream\_app:** query 1 đọc Kafka và ghi bronze (trigger 10 giây, checkpoint trong lake); query 2 tính cửa sổ event-time 1 phút với watermark 2 phút, chế độ `update`, ghi vào Postgres bằng `foreachBatch` với `ON CONFLICT DO UPDATE` để chạy lại không bị nhân đôi.
- **Grafana:** dashboard *Business KPIs* (ô doanh thu và tỉ lệ chuyển đổi, chuỗi theo ngày, cột theo brand, funnel, bảng top sản phẩm) và dashboard *Live stream* (làm mới 5 giây, kèm ô độ trễ). Biến lọc: brand, `category_l1`, khoảng thời gian. Datasource và dashboard nằm trong repo dưới dạng file provisioning.
- **rules\_worker:** script Python poll Postgres mỗi 30 giây, chỉ hai luật:

| Luật | Điều kiện | Hành động | Cooldown |
| --- | --- | --- | --- |
| `conversion_drop` | Purchase/view của 3 cửa sổ liên tiếp thấp hơn một nửa mức nền (trung bình 30 phút trước đó), và đủ lượng view | Tin nhắn Telegram | 15 phút |
| `abandoned_cart_spike` | Số giỏ bỏ dở của một brand trong 30 phút vượt ngưỡng | Telegram kèm danh sách `user_session` cần nhắc | 30 phút |

Mọi hành động ghi vào `action_log` với ràng buộc duy nhất (`rule_id`, khóa cửa sổ) để không gửi lặp. Có chế độ `dry_run` (chỉ ghi log) cho test; token Telegram đặt trong biến môi trường.

## 6. Lộ trình lean: làm lát cắt mỏng trước

Quy tắc chính: **đi hết đường ống ở mức mỏng nhất trước, rồi mới làm sâu từng khúc.** Điều này tránh tình trạng làm Kafka thật kỹ rồi mới phát hiện Spark không kết nối được.

| Phase | Việc | Xong khi |
| --- | --- | --- |
| P1. Lát cắt mỏng | Compose chạy Kafka, Spark, Postgres, Grafana; producer gửi 1.000 event; `stream_app` ghi bronze; silver và gold chạy tối thiểu; Grafana hiện một con số | Một event đi từ producer đến dashboard; đã kiểm tra RAM thật so với ngân sách ở mục 3 |
| P2. Generator và ingestion | Hai chế độ, điều khiển tốc độ, key theo session, có `flush()`, log throughput mỗi giây | Tốc độ thật bám tốc độ cấu hình; tổng offset Kafka bằng số event đã gửi |
| P3. Silver | Quy tắc R1–R5, quarantine, báo cáo chất lượng trước/sau, đối soát | 129.810 dòng silver khớp; chạy lại hai lần không đổi |
| P4. Gold và dashboard | Các bảng gold, ghi Postgres, dashboard *Business KPIs* với bộ lọc | Số liệu dashboard khớp truy vấn trực tiếp trên silver |
| P5. Streaming và hành động | Query cửa sổ 1 phút, dashboard *Live*, `rules_worker`, Telegram, kịch bản demo | Kịch bản `cart_abandon_spike` tạo ra tin nhắn và dòng trong `action_log` |
| P6. Thí nghiệm và báo cáo | X1–X5, README, báo cáo, phụ lục nhật ký AI | Có bảng và biểu đồ thí nghiệm, README chạy lại được |

Bốn quy tắc chống over-engineering:

1. Mỗi phase kết thúc bằng thứ chạy được và có bằng chứng (log, ảnh, số đo) lưu ngay vào `experiments/`.
2. Sau P1 không thêm công nghệ mới, trừ khi một lỗi hoặc một thí nghiệm chứng minh là cần.
3. Kẹt hơn một ngày thì đơn giản hóa hoặc cắt, rồi ghi lý do vào `docs/decisions.md`.
4. Mỗi quyết định kỹ thuật viết 5–10 dòng: bối cảnh, phương án, lý do chọn, đánh đổi.

## 7. Thí nghiệm (sáu, thay cho chín)

Mỗi thí nghiệm chạy ít nhất 3 lần, lấy trung vị, ghi cấu hình máy và phiên bản phần mềm.

| Mã | Câu hỏi | Biến thay đổi | Số đo |
| --- | --- | --- | --- |
| X1 | Khi nào Spark đáng dùng hơn pandas? | 130 nghìn, 1, 5, 10 triệu dòng (25 triệu tùy chọn) | Thời gian silver + gold, RAM đỉnh, điểm giao cắt |
| X2 | Định dạng và partition ảnh hưởng gì? | CSV, Parquet, Parquet partition theo ngày, có và không `repartition` | Dung lượng, số file, thời gian truy vấn theo ngày |
| X3 | Ingestion đạt tốc độ bao nhiêu? | 1.000, 5.000, 20.000 event/giây × 1 và 3 partition | Throughput thật, consumer lag |
| X4 | Độ trễ đầu cuối | 5.000 event/giây | p50, p95 từ `producer_ts` đến lúc ghi vào Postgres |
| X5 | Chịu lỗi | Kill `stream_app` giữa chừng rồi chạy lại | Số dòng đối soát, không mất, không trùng |
| X6 (tùy chọn) | Thêm nhân có giúp không? | `spark.cores.max` 1, 2, 4 trên silver 10 triệu dòng | Thời gian job |

Ghi chú trung thực: chạy trên một máy nên mọi tăng giảm tài nguyên chỉ là mô phỏng scale-out; nêu rõ giới hạn này và giải thích vì sao kết quả có thể không tăng tuyến tính.

## 8. Đối soát và kịch bản demo

**Đối soát (tự động hóa thành một script):** số event producer đã gửi = tổng offset Kafka = dòng bronze + dòng reject; bronze − quarantine − trùng = silver; tổng views, carts, purchases và doanh thu của gold khớp silver; số dòng Postgres khớp gold.

**Demo, tám bước:**

1. `docker compose up -d`, chụp trạng thái container.
2. Khởi động `stream_app`, mở dashboard *Live stream*.
3. Phát dữ liệu chế độ `live` ở 5.000 event/giây, chụp throughput và lag.
4. Cho xem file Parquet trong lake, truy vấn lại bằng Spark SQL.
5. Chạy `silver_job`, hiện báo cáo chất lượng trước/sau.
6. Chạy `gold_job`, mở *Business KPIs*, đổi bộ lọc brand.
7. Phát kịch bản `cart_abandon_spike`, chờ tin nhắn Telegram và dòng `action_log`.
8. Kill `stream_app`, chạy lại, chạy script đối soát.

## 9. Rủi ro còn lại

- Image Docker có thể không còn tải được hoặc đổi chính sách (nhất là các image Bitnami và MinIO); kiểm tra ở P1 và dùng image chính thức của Spark/Kafka.
- Cấu hình listener của Kafka trong Docker (địa chỉ trong mạng Docker và địa chỉ từ máy host) là lỗi kết nối hay gặp.
- Phiên bản gói Kafka source và driver JDBC phải khớp Spark (và S3A khớp Hadoop nếu bạn nâng cấp lên MinIO).
- Đồng hồ phải thống nhất khi đo độ trễ; `producer_ts` và thời điểm ghi Postgres lấy cùng nguồn thời gian.
- Đổi schema thì không dùng lại thư mục checkpoint cũ.
- Ổ đĩa đầy khi chạy quy mô lớn; Kafka và lake có thể chiếm vài GB.

## 10. Định nghĩa hoàn thành

- **Phải có:** P1–P4 và P6 xong; pipeline chạy từ nguồn đến dashboard Business KPIs với đối soát khớp; thí nghiệm X1–X5 có số đo thật; README chạy bằng `docker compose` kèm vài lệnh ghi rõ; nhật ký dùng AI; bạn giải thích được bằng lời của mình vì sao có từng thành phần và mỗi số đo nghĩa là gì.
- **Nên có:** P5 (Live dashboard, rule engine, Telegram).
- **Có thì tốt:** MinIO/S3A, RFM + k-means, X6, quy mô 25 triệu.
