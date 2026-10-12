# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Người Bi Ẩn |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602543 |
| GitHub | nguoibian863-ai |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-02134 |

## Kết quả cuối

- `dataset_version` của bạn: `1b413860aae7` (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 743 / 141 / 135 (tổng 1019)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_crash_log.png`, `airflow_crash_graph.png`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt chính giữa telemetry nguồn A và nguồn B được ghi nhận tại tầng Bronze:
1. Định dạng timestamp: Nguồn A sử dụng chuỗi thời gian chuẩn ISO 8601 UTC (ví dụ: `2025-10-07T08:00:00.014Z`), trong khi nguồn B sử dụng số nguyên Unix epoch theo mili-giây (ví dụ: `1759933801509`).
2. Tên cột và đơn vị vận tốc: Nguồn A dùng `speed_mps` với đơn vị mét/giây (ví dụ: 1.213 m/s), còn nguồn B dùng `spd_kmh` với đơn vị km/h (ví dụ: 55.11 km/h).
3. Đơn vị bàn đạp và góc lái: Nguồn A dùng `gas` và `brake` chuẩn hóa trong đoạn [0.0, 1.0] cùng `steer` (radian), trong khi nguồn B dùng tỉ lệ phần trăm `throttle_pct`, `brake_pct` [0..100] cùng `steering`. Tên các cột khác cũng khác biệt (`heading_deg` vs `hdg`, `lat`/`lon` vs `latitude`/`longitude`, `alt_m` vs `altitude`).
4. Độ lệch đồng hồ (clock drift) và tần số lấy mẫu giữa hai hệ thống ghi nhận khác nhau.

Pipeline giữ mỗi nguồn một bảng bronze và để mọi cột ở dạng chữ (raw text/string) theo nguyên lý Bronze Ingestion (Raw Lakehouse): việc không ép kiểu hay gộp sớm giúp bảo toàn 100% dữ liệu gốc (fidelity), tránh làm gián đoạn ingestion khi có dữ liệu format dị thường, cho phép replay, audit và xử lý lại khi các quy tắc nghiệp vụ ở Silver thay đổi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline chuẩn hóa schema bằng các mô hình dbt ở tầng Silver:
- Quy đổi thời gian về timestamp UTC chuẩn hóa ISO 8601; vận tốc quy đổi toàn bộ về mét/giây (`speed_mps = spd_kmh / 3.6` đối với drive B); ga/phanh chuẩn hóa về dải [0.0, 1.0] (`throttle_pct / 100.0`); tên các trường tọa độ và góc lái được mapping đồng nhất.
- Bù độ lệch đồng hồ (clock offset calibration) dựa vào thông tin offset của từng drive/trip để timeline telemetry khớp chính xác với timeline của video camera.
- Bằng chứng đúng: Các bài dbt test (schema & data tests) đạt 100%, bảng `silver.telemetry` có schema thống nhất, vận tốc nằm trong dải [0, 70] m/s và timestamp tăng đơn điệu.

Bảng quarantine (lấy từ `run_summary.json`):
| Drive | Lý do cách ly (quarantine_reason) | Số dòng |
|---|---|---|
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_a | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |

Tổng cộng có 106 dòng bị đưa vào bảng `rejects`. Dòng lỗi được đưa vào `rejects` thay vì xóa bỏ trực tiếp để tuân thủ quy tắc Quarantine Pattern: giúp kỹ sư dữ liệu giám sát tỷ lệ lỗi (data observability), tìm hiểu nguyên nhân gốc (hỏng cảm biến, mất gói mạng), và có thể re-process dữ liệu khi sửa quy tắc mà không bị thất thoát dữ liệu gốc.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` là `warn` vì việc mất tín hiệu định kỳ ngắt quãng nhẹ (khoảng cách giữa 2 bản ghi telemetry lớn hơn thông thường) là hiện tượng vật lý bình thường khi xe đi qua hầm hoặc nghẽn mạng ngắn hạn; dữ liệu xung quanh vẫn có giá trị và có thể nội suy được mà không làm hỏng toàn bộ pipeline. Trong khi đó, `in_range` (như vận tốc âm hoặc vượt quá 70 m/s ~ 250 km/h) là vi phạm logic vật lý nghiêm trọng (`error`), nếu để lọt vào training sẽ làm sai lệch phân phối dữ liệu và mô hình AI bị suy thoái.

Trong thí nghiệm `break-gate` (`break_gate.json`): Khi nới lỏng ngưỡng max speed lên 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` đã kích hoạt lỗi bắt được đúng 6 dòng vi phạm (kết quả từ PASS=6 chuyển sang PASS=5 WARN=1 ERROR=1, `gate_failed: true`). Điều này chứng minh vai trò tối quan trọng của "cổng chất lượng" (quality gate) như một cầu dao tự ngắt (circuit breaker), ngăn chặn dữ liệu độc hại lan xuống tầng Gold và mô hình học máy.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào `idempotency.json`:
- Trước khi chạy lại: 1246 dòng, fingerprint: `6c7480dfde7e`.
- Sau hai lần chạy lại liên tiếp: vẫn giữ nguyên đúng 1246 dòng, fingerprint không đổi `6c7480dfde7e` (`unchanged: true`). Trip thử nghiệm (`trip-02134`) có 60 dòng.
- Khi chạy thử với kiểu ghi `append`: số dòng của trip tăng gấp đôi lên 120 dòng (và toàn bảng bị nhân đôi dòng tương ứng).

Ghi `overwrite` theo partition trong Delta Lake an toàn hơn xoá rồi ghi lại vì Delta Lake sử dụng cơ chế transaction log (ACID commit): thao tác ghi đè phân vùng là atomic (nguyên tử). Nếu quá trình ghi bị lỗi giữa chừng (crash, OOM, mất điện), trạng thái trước đó của partition vẫn được bảo toàn nguyên vẹn, các truy vấn đọc không bao giờ thấy trạng thái bảng bị rỗng hay thiếu dòng. Ngược lại, nếu thực hiện `delete` rồi `insert`, việc crash sau khi vừa xóa sẽ làm mất vĩnh viễn dữ liệu của partition đó.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

Khung hình đen (black frame) là khung hình hỏng hoàn toàn (che ống kính, mất tín hiệu cảm biến camera), giá trị pixel trung bình ≈ 0 và variance ≈ 0, không chứa thông tin thị giác. Pipeline loại bỏ hoàn toàn (discard). Khung hình tối (dark frame) là khung hình chụp ban đêm hoặc trong hầm có ánh sáng yếu; khung hình vẫn chứa thông tin hữu ích (vạch kẻ đường, đèn xe, biển báo). Pipeline giữ lại (`dark_kept`: 4 ở drive_a, 61 ở drive_b). Tôi hoàn toàn đồng ý vì xe tự lái bắt buộc phải học lái an toàn trong điều kiện thiếu sáng ban đêm.

Số khung hình bị coi là trùng theo `threshold.json`:
| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2** (khớp với `threshold_in_use` của pipeline). Lý do: Ngưỡng 0 quá nghiêm ngặt, bỏ sót nhiều khung hình xe đứng yên nhưng cảm biến có nhiễu vi mô; ngưỡng 6 quá lỏng, loại bỏ tới hơn một nửa số khung hình (363 ở A, 244 ở B), làm mất cả những khung hình xe đang lăn bánh. Ngưỡng 2 lọc sạch các khung hình xe đứng chờ đèn đỏ mà vẫn giữ trọn các cảnh chuyển động. Ngưỡng này phụ thuộc chặt chẽ vào vận tốc: khi xe chạy nhanh, dHash giữa các khung hình kề nhau lớn; khi xe dừng đèn đỏ hoặc bò chậm, dHash nhỏ (<= 2).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Số liệu từ `leakage.json`:
- Khi chia theo khung hình (`per_frame_split`): Có **26/26** trips (100%) bị chia cắt (`per_frame_split_trips_straddling = 26`), và toàn bộ 26 trips đều xuất hiện đồng thời ở cả tập Train và tập Test (`per_frame_split_trips_in_both_train_and_test = 26`).
- Khi chia theo trip (`per_trip_split`): Có đúng **0** trip bị chia cắt (`per_trip_split_trips_straddling = 0`). Mọi khung hình của cùng một trip chỉ nằm gọn trong một split duy nhất.

Điều này làm điểm kiểm thử đẹp giả tạo vì hiện tượng rò rỉ dữ liệu (data leakage) giữa các khung hình liền kề trong cùng một video (temporal correlation): hai khung hình cách nhau vài chục mili-giây có hậu cảnh, điều kiện thời tiết, góc máy và vật thể gần như y hệt. Nếu để khung hình của cùng một chuyến đi vào cả tập Train và Test, mạng nơ-ron chỉ cần "học vẹt" (memorize) bối cảnh cụ thể đó là đã đoán đúng ở tập Test. Kết quả kiểm thử trên giấy rất cao nhưng khi triển khai xe chạy trên một cung đường mới ngoài đời thực, mô hình sẽ thất bại nặng nề.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là băm nội dung (content-based hashing) để đảm bảo tính tất định (determinism) và idempotent: nếu dữ liệu đầu vào, các quy tắc lọc và phân chia split không thay đổi, mã hash nội dung sẽ luôn cố định (`1b413860aae7`). Nếu dùng timestamp hay số tự tăng, mỗi lần chạy lại đều tạo ra một version mới dù dữ liệu hoàn toàn cũ, làm mất tính bất biến và lãng phí tài nguyên quản lý. Hai lần chạy liên tiếp cùng dữ liệu cho ra cùng một mã hash không đổi.

`dataset_version` của tôi khác bạn bè dù cùng dữ liệu thô ban đầu vì pipeline sử dụng MSSV (`LAB_STUDENT_ID = 2A202602543`) làm `seed` ngẫu nhiên để phân chia các trip vào các tập Train / Val / Test. Mỗi sinh viên có MSSV riêng nên việc phân bổ các trip vào các tập split là khác nhau, dẫn tới manifest và nội dung dataset cuối cùng có mã hash khác biệt duy nhất.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Nội dung lineage trong `manifest.json`:
Dataset này được tạo từ:
- Dữ liệu video và telemetry thô: `drive_a.mp4`, `drive_b.mp4`, `telemetry_a.csv`, `telemetry_b.csv` với các mã băm SHA-256 gốc được kiểm chứng ở tầng Bronze.
- Các quy tắc chuẩn hóa Silver: lọc tốc độ hợp lệ [0, 70] m/s, dbt models `silver_telemetry` và `silver_frames_flagged` với dung sai đồng bộ thời gian camera-telemetry `sync_tol_ms = 100`.
- Ngưỡng lọc trùng lặp hình ảnh: dHash diff threshold <= 2 bits, lọc khung hình đen có tỷ lệ điểm tối > 0.95.
- Phân chia split theo trip dựa trên seed MSSV `2A202602543` với tỷ lệ 70% Train, 15% Val, 15% Test.

Sau 3 tháng, những thông tin cần thiết để tái tạo lại chính xác 100% dataset này bao gồm:
1. Giá trị SHA-256 của các tệp video và telemetry gốc.
2. Commit hash Git của repository mã nguồn (chứa toàn bộ code pipeline, dbt models, config `dbt_project.yml`).
3. Giá trị `LAB_STUDENT_ID` trong file cấu hình `.env` (`2A202602543`).

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Khi thiết lập `LAB_FAIL_ONCE=trip-02134` và kích hoạt DAG `day09_video_pipeline`:
- Đúng 1 task map instance trong `silver_frames` ứng với `trip-02134` (map_index=5) bị failed chuyển sang màu đỏ với thông báo lỗi mô phỏng: `RuntimeError: simulated crash while extracting trip-02134`.
- Ba task tiếp theo ở downstream (`dbt_test_frames`, `gold_dataset`, `publish_marts`) bị chuyển sang trạng thái `upstream_failed` (màu cam). 25 task map instances khác của các trip còn lại vẫn chạy thành công (màu xanh).
- Tôi thực hiện thao tác Clear đúng task bị đỏ (`silver_frames` map_index=5). Task này chạy lại thành công, kéo theo các downstream task chạy tiếp và toàn bộ DAG chuyển xanh hoàn toàn.
- Dữ liệu hoàn toàn không bị nhân đôi nhờ cơ chế partition overwrite idempotent của Delta Lake. `dataset_version` vẫn giữ nguyên là `1b413860aae7`.
- Bằng chứng: Ảnh chụp log task sập (`report/screenshots/airflow_crash_log.png`), ảnh DAG graph lúc task bị đỏ và sau khi DAG xanh (`report/screenshots/airflow_crash_graph.png`, `report/screenshots/airflow_graph.png`), và kết quả kiểm tra `report/verify.txt` (27/27 passed).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba thành phần sẽ vỡ đầu tiên khi nâng quy mô lên 500 xe/ngày:
1. Bộ nhớ và công cụ tính toán đơn máy (DuckDB & Single Host Storage - Slide 12, 22):
   DuckDB chạy trên một máy chủ cục bộ sẽ cạn kiệt dung lượng đĩa và RAM khi xử lý hàng trăm terabytes video và telemetry mỗi ngày. Giải pháp: Chuyển sang kiến trúc Distributed Lakehouse trên Cloud Object Storage (Amazon S3 / GCS) với định dạng Apache Iceberg hoặc Delta Lake, kết hợp engine xử lý phân tán như Apache Spark hoặc Databricks / Ray.
2. Bộ giải nén video và trích xuất khung hình bằng CPU tuần tự (OpenCV on Airflow Worker - Slide 22, 23):
   Việc decode hàng nghìn giờ video bằng CPU trên worker của Airflow sẽ gây nghẽn nghiêm trọng (compute bottleneck). Giải pháp: Tách việc trích xuất frame thành cụm dịch vụ serverless/batch chuyên dụng (Kubernetes Job / Ray Data) sử dụng phần cứng tăng tốc GPU (NVIDIA NVDEC qua FFmpeg GPU).
3. Ingestion telemetry theo file lô định kỳ (Batch CSV Ingestion - Slide 12, 23):
   Thu gom CSV theo lô sẽ tạo độ trễ lớn và rủi ro nghẽn IO khi hàng trăm xe cùng tải lên. Giải pháp: Kiến trúc Streaming Ingestion, đưa dữ liệu telemetry trực tiếp qua giao thức IoT/MQTT vào Apache Kafka hoặc AWS Kinesis, sau đó dùng Apache Flink / Spark Streaming để làm sạch và ghi liên tục vào Bronze/Silver theo thời gian thực.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT 
    trip_id,
    drive_id,
    total_frames,
    kept_frames,
    rejected_frames,
    ROUND(100.0 * rejected_frames / NULLIF(total_frames, 0), 2) AS reject_rate_pct
FROM marts.mart_trip_quality_summary
ORDER BY reject_rate_pct DESC
LIMIT 10;
```

Điều truy vấn này cho biết:
Truy vấn này xếp hạng 10 chuyến đi (trips) có tỷ lệ khung hình bị loại bỏ (reject rate %) cao nhất trong toàn bộ hệ thống. Điều này giúp kỹ sư dữ liệu và chuyên gia thị giác máy tính nhận diện ngay các chuyến đi có camera bị lỗi kỹ thuật, tài xế dừng chờ đèn đỏ quá lâu (sinh ra lượng lớn near-duplicate frames), hoặc xe đi qua các khu vực hầm/đoạn đường thiếu sáng nghiêm trọng. Từ đó giúp tối ưu hóa việc phân bổ xe và lập kế hoạch thu thập dữ liệu hiệu quả hơn.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là sức mạnh của tính Idempotent và cơ chế partition overwrite trong Delta Lake: khi một task trích xuất bị crash giữa chừng, pipeline khôi phục mượt mà mà không để lại bất kỳ dữ liệu rác hay nhân đôi nào. Điểm còn muốn tìm hiểu sâu hơn là cách tối ưu chi phí và độ trễ khi tính toán vector embedding hàng triệu khung hình theo thời gian thực trên đám mây. Một lỗi dữ liệu tiềm ẩn mà pipeline hiện tại chưa bắt được là hiện tượng camera bị bám bẩn hoặc vật thể che khuất một phần (partial occlusion/smudge): khung hình không đủ tối để coi là black, không bị mờ nhòe toàn cục (blur) và dHash vẫn đổi theo chuyển động xe, nhưng ground truth vật thể bị biến dạng gây suy giảm độ chính xác của mô hình phát hiện vật thể.
