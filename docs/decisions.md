# Nhật ký quyết định kỹ thuật (decisions.md)

Tệp này ghi các quyết định **thay đổi hoặc làm rõ** so với bản kế hoạch chi tiết, phát sinh từ bản review kỹ thuật. Các phần của kế hoạch không được nhắc ở đây giữ nguyên.

Quy ước mỗi quyết định: bối cảnh, phương án, quyết định, đánh đổi. Trạng thái:

- **Đề xuất**: cần bạn xác nhận hoặc sửa trước khi code.
- **Tạm thời**: giá trị khởi đầu, sẽ chốt sau khi có số đo thật.

Các con số đánh dấu *(khởi đầu)* chỉ là giá trị bắt đầu, phải hiệu chỉnh bằng số đo ở P1 hoặc thí nghiệm liên quan.

| Mã | Chủ đề | Mức ưu tiên | Trạng thái |
| --- | --- | --- | --- |
| D-01 | Silver và gold chạy full refresh | P0 | Đề xuất |
| D-02 | Grain bảng gold | P0 | Đề xuất |
| D-03 | Ba chế độ dữ liệu và manifest | P0 | Đề xuất |
| D-04 | Query streaming: khử trùng, watermark, dung sai | P0 | Đề xuất |
| D-05 | Định nghĩa độ trễ và lag | P0 | Đề xuất |
| D-06 | Tách P5 thành P5a và P5b | P0 | Đề xuất |
| D-07 | Luật `abandoned_cart_spike` dùng tín hiệu streaming | P0 | Đề xuất |
| D-08 | Ngân sách tài nguyên và mạng driver Spark | P0 | Đề xuất |
| D-09 | Cấu hình Kafka source, topic và checkpoint | P1 | Tạm thời |
| D-10 | Data contract, thứ tự quy tắc, tie-breaker | P1 | Đề xuất |
| D-11 | Tên và định nghĩa hai metric giỏ hàng | P1 | Đề xuất |
| D-12 | Cách ghi vào Postgres | P1 | Đề xuất |
| D-13 | Chiến lược partition và file nhỏ | P1 | Tạm thời |
| D-14 | Điều phối job batch bằng Compose | P1 | Đề xuất |
| D-15 | Kế hoạch kiểm thử tối thiểu | P1 | Đề xuất |
| D-16 | Quy ước chạy thí nghiệm | P2 | Đề xuất |
| D-17 | Cờ ngoại lệ giá | P2 | Đề xuất |

---

## D-01. Silver và gold chạy full refresh từ bronze

**Bối cảnh.** Kế hoạch mô tả bronze partition theo `ingest_date`, silver partition theo `event_date` và "ghi đè theo partition nên chạy lại không bị nhân đôi", nhưng không nói job đọc toàn bộ hay chỉ phần mới. Nếu chỉ đọc partition bronze mới rồi ghi đè theo `event_date`, dữ liệu của các lần ingest trước trong cùng ngày bị mất, và bản trùng nằm ở hai ngày ingest khác nhau không bị khử.

**Phương án.**
1. Incremental theo `ingest_date`, cần `partitionOverwriteMode=dynamic` và đọc lại mọi partition bronze liên quan.
2. Full refresh: mỗi lần chạy đọc toàn bộ bronze, tính lại silver rồi gold.

**Quyết định.** Chọn phương án 2. Silver và gold luôn tính lại từ toàn bộ bronze. Nếu ghi lại theo đường dẫn có partition thì đặt tường minh `spark.sql.sources.partitionOverwriteMode` và kiểm tra hành vi trên dữ liệu nhỏ trước khi chạy dữ liệu lớn.

**Đánh đổi.** Tốn thời gian chạy hơn khi dữ liệu lớn, nhưng ở quy mô 10 triệu dòng trên một máy vẫn ở mức vài phút. Đổi lại idempotency đúng theo định nghĩa (cùng đầu vào, cùng đầu ra) và đối soát đơn giản. Incremental chuyển sang P2 nếu còn thời gian.

---

## D-02. Grain bảng gold phục vụ bộ lọc dashboard

**Bối cảnh.** `kpi_daily` chỉ có chiều ngày và `kpi_brand_funnel` chỉ có chiều brand, trong khi dashboard cần lọc theo brand, `category_l1` và khoảng thời gian. Tỉ lệ lưu sẵn thành cột sẽ sai khi tổng hợp lại.

**Phương án.**
1. Giữ các bảng riêng và thêm cột khi cần.
2. Một bảng fact với grain đủ chi tiết, tính tỉ lệ ở truy vấn.

**Quyết định.** Chọn phương án 2. Bảng `kpi_fact` có grain `(event_date, brand, category_l1)` với các số đo cộng được: `views`, `carts`, `purchases`, `revenue`. Tỉ lệ luôn tính bằng `SUM(a) / NULLIF(SUM(b), 0)` trong truy vấn Grafana. `kpi_brand_funnel` trở thành view hoặc truy vấn trên `kpi_fact`. `top_products` thêm `event_date` để lọc theo thời gian.

**Đánh đổi.** Bảng lớn hơn và truy vấn phức tạp hơn một chút, nhưng số liệu đúng với mọi tổ hợp bộ lọc. Quy mô (số ngày × brand × category) vẫn nhỏ với Postgres.

---

## D-03. Ba chế độ dữ liệu và `manifest.json`

**Bối cảnh.** Dữ liệu gốc đã có lỗi tự nhiên (thiếu session, trùng, thiếu category). Nếu generator chèn thêm lỗi lên chính dữ liệu gốc thì mốc 129.810 dòng silver không còn đúng. Ngoài ra không có ground truth để kiểm tra quy tắc silver, và KPI trên dữ liệu synthetic phản ánh tham số generator chứ không phải insight thật.

**Quyết định.** Generator có ba chế độ:

- `replay`: phát lại nguyên bản dữ liệu gốc, không chèn lỗi. Kỳ vọng silver = 129.810, các số mẫu ở mục 4 dùng để đối chiếu.
- `synthetic`: sinh dữ liệu lớn theo phân phối lấy từ dữ liệu thật, có chèn lỗi theo tỉ lệ cấu hình. Dùng cho thí nghiệm quy mô và chịu lỗi.
- `live`: như synthetic nhưng `event_time = bây giờ`, dùng cho dashboard Live và demo.

Mỗi lần chạy `synthetic` và `live` xuất `manifest.json` gồm: seed, số event, khoảng thời gian, số lỗi đã chèn theo từng loại (thiếu session, trùng, JSON hỏng, giá không hợp lệ, event trễ). Đối soát so quarantine với manifest. Generator tất định theo seed.

**Đánh đổi.** Thêm công sức cho generator và manifest. Đổi lại kiểm chứng được quy tắc bắt đủ và không bắt nhầm. Trong README ghi rõ: KPI "thật" chỉ lấy từ `replay`; dữ liệu synthetic chỉ phục vụ kiểm thử.

---

## D-04. Query streaming: khử trùng, watermark và dung sai đối soát

**Bối cảnh.** Query cửa sổ 1 phút đọc trực tiếp từ Kafka, nên bản trùng (0,09%) bị đếm hai lần. Watermark 2 phút ngắn hơn độ trễ tối đa của dữ liệu (3 phút), nên một phần event trễ bị bỏ khỏi cửa sổ và `kpi_stream_1min` không bao giờ khớp gold.

**Quyết định.**
- Khử trùng theo `event_id` trong query 2 (dùng `dropDuplicatesWithinWatermark` hoặc `dropDuplicates` kèm cột event-time và watermark; xác nhận API trong tài liệu Spark 3.5 khi cài).
- Watermark đặt bằng độ trễ tối đa của generator: 3 phút *(khởi đầu)*.
- Đối soát stream so với silver có **dung sai**: stream phải đạt tối thiểu 99% số event của silver cho cùng các cửa sổ đã đóng *(khởi đầu, hiệu chỉnh ở P5a)*. Phần chênh lệch do event trễ quá watermark được ghi vào báo cáo như một đặc tính của streaming.
- Query 2 chỉ dùng với dữ liệu `live`. Chế độ `replay` và `synthetic` phục vụ nhánh batch.
- Upsert vào Postgres đặt giá trị tuyệt đối của cửa sổ (không cộng dồn), vì chế độ `update` phát ra tổng hiện tại.

**Đánh đổi.** Watermark dài hơn giữ state lâu hơn (không đáng kể ở quy mô này). Chấp nhận stream và batch không khớp tuyệt đối, đổi lại không phải dùng cơ chế phức tạp hơn.

---

## D-05. Định nghĩa độ trễ và lag

**Bối cảnh.** X4 đo từ `producer_ts` đến lúc ghi Postgres nhưng bảng `kpi_stream_1min` chỉ chứa số đếm theo cửa sổ, không có `producer_ts` ở cấp event. Offset của Spark nằm trong checkpoint, mặc định không commit vào consumer group, nên công cụ xem lag của consumer group không cho số đúng cho ứng dụng Spark.

**Quyết định.**
- Bronze luôn giữ `producer_ts` và `raw_value` (chuỗi thô).
- **Ingest latency** (cấp event) = `ingest_ts − producer_ts`; báo cáo p50 và p95. Đây là số dùng cho X4.
- **Freshness** (cấp dashboard) = `now() − thời điểm ghi gần nhất của cửa sổ mới nhất`; dùng cho ô độ trễ trên dashboard Live.
- **Lag** lấy từ tiến trình của streaming query (số offset còn lại so với mới nhất), ghi ra log hoặc một bảng nhỏ. Không dựa vào lệnh xem consumer group. Xác nhận tên chỉ số trong tài liệu Spark khi cài.
- Mọi thời điểm lấy từ cùng một nguồn đồng hồ (cùng máy, UTC).

**Đánh đổi.** Độ trễ của kết quả tổng hợp bị chi phối bởi chu kỳ trigger, nên báo cáo nêu rõ đây là độ trễ ingest và độ tươi, không phải độ trễ từng event đến dashboard.

---

## D-06. Tách P5 thành P5a và P5b

**Bối cảnh.** X4 và X5 phụ thuộc query cửa sổ 1 phút (thuộc P5) nhưng mục 10 xếp P5 vào "Nên có", còn X1–X5 vào "Phải có".

**Quyết định.**
- **P5a** (bắt buộc): query cửa sổ 1 phút, `kpi_stream_1min`, dashboard Live, đo độ trễ và lag.
- **P5b** (nên có): `rules_worker`, Telegram, kịch bản demo.
- Mục 10 cập nhật: "Phải có" gồm P1–P4, P5a, P6; "Nên có" gồm P5b.

**Đánh đổi.** Khối lượng bắt buộc tăng một phần, nhưng X4 và X5 có ý nghĩa đầy đủ. Nếu thiếu thời gian, cắt P5b trước.

---

## D-07. Luật `abandoned_cart_spike` dùng tín hiệu streaming theo brand

**Bối cảnh.** `kpi_stream_1min` có khóa `window_start + event_type`, không có brand hay session. `abandoned_carts` là bảng gold do batch sinh ra và chỉ biết "bỏ dở" sau 30 phút chờ, nên cảnh báo luôn trễ ít nhất 30 phút, không demo trực tiếp được.

**Phương án.**
1. Định nghĩa lại luật theo tín hiệu streaming.
2. Giữ định nghĩa 30 phút, rút ngắn ở chế độ demo, kèm bảng trạng thái cart theo session.

**Quyết định.** Chọn phương án 1.
- Thêm `brand` vào khóa của `kpi_stream_1min`: `(window_start, brand, event_type)`.
- Luật: tỉ lệ `carts` so với `purchases` của một brand trong các cửa sổ gần nhất vượt ngưỡng so với mức nền, kèm lượng cart tối thiểu. Ngưỡng và độ dài cửa sổ hiệu chỉnh ở P5b.
- Tin nhắn Telegram bỏ danh sách `user_session`.
- `rules_worker` chỉ đọc các cửa sổ đã đóng, bỏ qua cửa sổ mới nhất đang mở.
- `conversion_drop` dùng cùng nguyên tắc cửa sổ đã đóng.
- `abandoned_carts` giữ nguyên là bảng phân tích batch, không dùng cho cảnh báo.

**Đánh đổi.** Mất khả năng nhắc đích danh từng session; đổi lại demo chạy được trong vài phút và thiết kế đơn giản hơn. Có thể quay lại phương án 2 ở P2.

---

## D-08. Ngân sách tài nguyên và mạng driver Spark

**Bối cảnh.** Bảng RAM mục 3 chỉ tính worker, không tính driver của `stream_app` và của job batch chạy cùng lúc. Với ứng dụng PySpark trên cluster standalone, driver chạy ở client mode (trong container submit) và executor phải kết nối ngược về driver. Executor 1 GB khá chật cho 10 triệu dòng.

**Quyết định.**
- Cập nhật bảng RAM với driver: mỗi driver tính khoảng 1 GB cộng overhead *(khởi đầu)*, và đo RSS thực tế ở P1.
- Nếu tổng vượt `memory=6GB` ở `.wslconfig`, hoặc tăng lên khi đóng được trình duyệt, hoặc không chạy batch lớn cùng lúc với stream.
- Cấu hình mạng driver là tiêu chí xong của P1: đặt `spark.driver.host`, `spark.driver.bindAddress`, cố định `spark.driver.port` và `spark.blockManager.port`, và mở các cổng đó trong mạng Docker Compose.
- Ghi nhận giới hạn executor 1 GB như một phát hiện thí nghiệm (X1, X6), không giấu đi.

**Đánh đổi.** Ít dư địa hơn cho chạy đồng thời. Chấp nhận để giữ phạm vi gọn.

---

## D-09. Cấu hình Kafka source, topic và checkpoint

**Bối cảnh.** Khi khởi động lại (X5) hoặc bắt đầu sau khi Kafka đã chứa nhiều event, batch đầu tiên cố đọc toàn bộ offset chưa xử lý. `retention.bytes` nhỏ kết hợp `failOnDataLoss` mặc định làm query dừng nếu Kafka xóa dữ liệu trước khi Spark đọc. Tạo lại topic mà giữ checkpoint cũ gây lỗi offset.

**Quyết định.**
- `maxOffsetsPerTrigger` = 200.000 *(khởi đầu, hiệu chỉnh ở P1 và X3)*.
- `startingOffsets=earliest` cho lần chạy đầu; sau đó checkpoint quyết định.
- `failOnDataLoss` giữ mặc định (`true`) để lỗi hiện ra thay vì mất dữ liệu âm thầm. Đặt `retention.bytes` đủ lớn so với dung lượng một lần chạy.
- Checkpoint đặt ngoài thư mục gốc của bronze, ví dụ `lake/_checkpoints/<tên query>`.
- Quy ước: **đổi topic hoặc đổi schema thì xóa checkpoint tương ứng**. Mỗi cấu hình X3 dùng một topic riêng (ví dụ `events.raw.p1`, `events.raw.p3`), vì Kafka không giảm được số partition.
- Producer: `acks=all`, `enable.idempotence=true`, `linger.ms` và nén cấu hình theo X3.

**Đánh đổi.** Batch nhỏ hơn làm tăng thời gian bắt kịp nhưng bảo vệ bộ nhớ. Các giá trị chốt lại sau khi đo.

---

## D-10. Data contract, thứ tự quy tắc và tie-breaker

**Bối cảnh.** JSON hỏng (0,05%) được generator sinh ra nhưng không có quy tắc nào xử lý. "Dòng reject" trong đối soát chưa được định nghĩa. Bronze vừa là chuỗi thô vừa có `event_id`. Thứ tự các quy tắc và việc giữ bản nào khi khử trùng chưa xác định, nên kết quả có thể không tất định.

**Quyết định.**
- **Bronze** lưu `raw_value` (chuỗi thô, không phụ thuộc parse), `kafka_partition`, `kafka_offset`, `ingest_ts`, `producer_ts` nếu đọc được. Việc parse, ép kiểu và tính `event_id` nằm ở silver.
- Viết một trang `docs/data_contract.md`: tên cột, kiểu, nullable, miền giá trị (đặc biệt `event_type`), múi giờ nguồn, schema `StructType` tường minh.
- Thêm **R0**: không parse được hoặc thiếu trường bắt buộc (`event_time`, `event_type`, `product_id`) thì đưa vào quarantine, lý do ghi rõ.
- Thứ tự áp dụng: R0, R1, R5, rồi R2 (khử trùng), rồi R3, R4. Mỗi dòng chỉ thuộc một lý do loại, theo thứ tự đầu tiên bị vi phạm.
- **Tie-breaker khi khử trùng:** giữ bản có (`kafka_partition`, `kafka_offset`) nhỏ nhất.
- Đối soát viết lại: `bronze = silver + quarantine (R0, R1, R5) + bản trùng bị loại (R2)`. "Reject" nghĩa là các dòng ở quarantine do R0.

**Đánh đổi.** Silver làm nhiều việc hơn, nhưng bronze bảo toàn mọi thứ và cho phép phát lại.

---

## D-11. Tên và định nghĩa hai metric giỏ hàng

**Bối cảnh.** Tỉ lệ purchase/cart theo event (khoảng 60,6%) và tỉ lệ giỏ bỏ dở (khoảng 58,6%) được tính trên cơ sở khác nhau nên cộng lại hơn 100%, dễ bị hiểu là mâu thuẫn. Nếu 60,6% tính trên cùng 10.924 cart thì có khoảng 6.620 purchase, trong khi chỉ khoảng 4.527 cart có purchase khớp (cùng session, cùng sản phẩm, trong 30 phút).

**Quyết định.**
- Metric A: **tỉ lệ purchase trên cart theo event** = tổng purchase chia tổng cart (không gắn từng giỏ).
- Metric B: **tỉ lệ giỏ bỏ dở theo cohort** = số cart không có purchase cùng session và sản phẩm trong 30 phút sau, chia tổng cart.
- Hai metric hiển thị cạnh nhau trên dashboard kèm chú thích về cơ sở tính. Báo cáo giải thích phần chênh lệch bằng hạn chế dữ liệu đã nêu (627 session có purchase mà không có cart, tracking giỏ hàng không đầy đủ).
- Giỏ hàng nằm trong 30 phút cuối của dữ liệu chưa thể phân loại, đánh dấu riêng hoặc loại khỏi phép tính.

**Đánh đổi.** Thêm một chú thích trên dashboard, đổi lại số liệu giải thích được khi bị hỏi.

---

## D-12. Cách ghi vào Postgres

**Bối cảnh.** Ghi JDBC chế độ overwrite mặc định xóa và tạo lại bảng (mất khóa chính và chỉ mục), và dashboard có lúc thấy bảng rỗng. Spark JDBC không có upsert.

**Quyết định.**
- DDL (bảng, khóa chính, chỉ mục theo thời gian) đặt trong script khởi tạo của Postgres, không để Spark tự tạo.
- `gold_job` ghi vào bảng staging, rồi trong một transaction thay thế bảng đích (truncate rồi insert, hoặc đổi tên). Dashboard không bao giờ thấy trạng thái nửa chừng.
- Upsert của `stream_app` thực hiện bằng SQL `ON CONFLICT DO UPDATE` qua thư viện Postgres cho Python (ví dụ psycopg) trong `foreachBatch`; thư viện được thêm vào image.

**Đánh đổi.** Thêm một thư viện và vài script SQL, bù lại không có khoảng trống dữ liệu và khóa chính luôn còn.

---

## D-13. Chiến lược partition và file nhỏ

**Bối cảnh.** 130 nghìn dòng chia theo ngày chỉ còn vài trăm dòng mỗi partition; kể cả sau `repartition` mỗi file vẫn rất nhỏ. Ở 10 triệu dòng (khoảng vài trăm MB đến 1 GB) vẫn nhỏ. Bronze với trigger 10 giây sinh tối đa 3 file mỗi batch mà chưa có bước gộp.

**Quyết định.** *Tạm thời:* silver và gold **không partition theo ngày** cho đến khi X2 cho thấy lợi ích. X2 thêm hai biến thể: "không partition, vài file lớn" và "partition theo tháng", ngoài các biến thể đã có. Bronze giữ partition `ingest_date`. Compaction bronze chuyển sang P2. Cập nhật mục này sau X2.

**Đánh đổi.** Truy vấn theo ngày có thể quét nhiều dữ liệu hơn, nhưng ở quy mô này chi phí đó nhỏ hơn chi phí của hàng trăm file nhỏ.

---

## D-14. Điều phối job batch bằng Compose

**Bối cảnh.** Silver, gold và đối soát phụ thuộc thứ tự nhưng hiện chỉ do người chạy tay từng lệnh. Airflow đã bị cắt có chủ đích.

**Quyết định.** Không dùng bộ điều phối. Một service `batch` (hoặc script `run_batch.sh`) chạy lần lượt `silver_job`, `gold_job`, đối soát; dùng `depends_on` với điều kiện `service_completed_successfully` nếu chạy qua Compose. Script đối soát trả mã thoát khác 0 khi lệch. Mỗi bước ghi một dòng log JSON: bước, số dòng vào, số dòng ra, thời gian chạy.

**Đánh đổi.** Không có lịch chạy hay giao diện theo dõi, nhưng đủ cho phạm vi dự án và dễ giải thích.

---

## D-15. Kế hoạch kiểm thử tối thiểu

**Bối cảnh.** Kế hoạch có đối soát end-to-end và thí nghiệm hiệu năng nhưng chưa có kiểm thử đơn vị, kiểm thử khói hay CI.

**Quyết định.**
- `pytest` cho các hàm thuần: chuẩn hóa brand, tính `category_l1`, sinh `event_id`, các điều kiện của luật; kèm kiểm tra generator tất định theo seed.
- Một mẫu dữ liệu 1–2 nghìn dòng trong `tests/` để chạy nhanh.
- Smoke test cho P1: 1.000 event đi từ producer đến một con số trên Grafana.
- GitHub Actions chỉ chạy lint và `pytest`; không chạy cả hệ thống Docker trên CI.
- Báo cáo DQ tự viết là đủ; không đưa thêm công cụ kiểm tra chất lượng dữ liệu.

**Đánh đổi.** Không bao phủ tích hợp trên CI, nhưng phần đó đã có đối soát và smoke test chạy cục bộ.

---

## D-16. Quy ước chạy thí nghiệm

**Bối cảnh.** Chạy thí nghiệm cùng lúc với stream làm nhiễu số đo. Ở 20.000 event/giây, nút cổ chai có thể là generator Python chứ không phải Kafka. Với Spark 2 nhân và executor 1 GB, pandas nhiều khả năng thắng trong toàn dải của X1.

**Quyết định.**
- Chạy thí nghiệm khi không có job nào khác; dừng `stream_app` và `rules_worker` khi đo X1, X2, X6.
- X1 thêm đối chứng Spark `local[*]` để tách chi phí framework khỏi chi phí cluster. Nếu pandas thắng đến 10 triệu dòng thì ghi đó là kết quả.
- X3 đo riêng công suất tối đa của generator (không gửi Kafka) trước khi kết luận về Kafka.
- Mỗi thí nghiệm ghi cấu hình máy, phiên bản phần mềm, và chạy ít nhất 3 lần lấy trung vị như kế hoạch.
- Mức 25–50 triệu dòng chỉ làm nếu X1 ở 10 triệu dòng chạy ổn trong ngân sách RAM.

**Đánh đổi.** Thí nghiệm tốn thời gian hơn vì chạy tuần tự, nhưng số đo tin cậy.

---

## D-17. Cờ ngoại lệ giá (R4)

**Bối cảnh.** Ngưỡng P99,9 do chính dữ liệu định nghĩa nên luôn gắn cờ đúng khoảng 0,1% dòng; đây không phải phát hiện chất lượng. Linh kiện đắt tiền có thể là giá thật. Ngưỡng tính lại mỗi lần chạy làm kết quả đổi theo dữ liệu.

**Quyết định.** Giữ `is_price_outlier` ở mức gắn cờ, không loại dòng (như kế hoạch), nhưng đưa vào P2 việc đổi sang so sánh giá với giá điển hình của chính sản phẩm đó trong catalog. Trong lúc chưa đổi, lưu ngưỡng vào cấu hình hoặc bảng nhỏ thay vì tính lại mỗi lần, và ghi trong báo cáo DQ rằng cờ này không phải chỉ số lỗi.

**Đánh đổi.** Cờ hiện tại ít giá trị phân tích, nhưng không gây hại vì dòng vẫn được giữ.

---

## Cập nhật cần làm trong kế hoạch gốc

- Mục 2: thêm `raw_value` và `producer_ts` vào bronze; thêm `brand` vào khóa `kpi_stream_1min`; ghi chú full refresh.
- Mục 3: cập nhật bảng RAM với driver (D-08).
- Mục 4: ba chế độ generator và `manifest.json` (D-03); grain gold mới (D-02); quy tắc R0 và thứ tự (D-10); tên hai metric (D-11).
- Mục 5: luật `abandoned_cart_spike` và cửa sổ đã đóng (D-07).
- Mục 6 và 10: tách P5a và P5b (D-06).
- Mục 7: định nghĩa độ trễ và lag, X2 thêm biến thể, X1 thêm đối chứng, X3 đo generator (D-05, D-13, D-16).
- Mục 8: đối soát có dung sai cho stream và công thức mới cho bronze, silver, quarantine (D-04, D-10).
