# Reflection

Anti-pattern dễ gặp nhất trong hệ thống quan sát LLM là **small files**. Dữ liệu request đến liên tục, nhiều tenant và model khiến một pipeline micro-batch dễ tạo hàng nghìn file Parquet nhỏ. Khi đó chi phí không chỉ nằm ở dung lượng: engine phải mở nhiều file, đọc nhiều metadata, lập kế hoạch chậm và dashboard có độ trễ thất thường. Partition quá chi tiết theo tenant hoặc model còn làm tình trạng nặng hơn.

Cách phòng tránh là partition theo trường có cardinality thấp và phù hợp truy vấn, chẳng hạn ngày; dùng clustering cho tenant/model; đặt mục tiêu kích thước file và chạy compaction theo backlog thay vì theo lịch cố định. Tôi sẽ theo dõi số file, kích thước p50/p95 và thời gian planning để kích hoạt maintenance, đồng thời dùng checkpoint và VACUUM theo retention an toàn. NB2 và NB6 cho thấy compaction giảm số file rõ rệt, còn clustering giúp file skipping mà không cần tạo partition cardinality cao.

Phạm vi sử dụng AI được công khai tại [AI_USAGE.md](AI_USAGE.md).
