# Metabase Dashboard cho Job Postings
---

## 1. Top 10 ngành nghề được đăng tuyển nhiều nhất
```sql
SELECT category, COUNT(*) AS job_count
FROM job_postings
WHERE category IS NOT NULL
GROUP BY category
ORDER BY job_count DESC
LIMIT 10;
```
**Visualization:** Bar Chart (Horizontal)

---

## 2. Số lượng tin tuyển dụng theo khu vực
```sql
SELECT region, COUNT(*) AS job_count
FROM job_postings
WHERE region IS NOT NULL
GROUP BY region
ORDER BY job_count DESC;
```
**Visualization:** Bar Chart (Horizontal)

---

## 3. Top 10 công ty có nhiều tin đăng nhất
```sql
SELECT company_name, COUNT(*) AS job_count
FROM job_postings
WHERE company_name IS NOT NULL
GROUP BY company_name
ORDER BY job_count DESC
LIMIT 10;
```
**Visualization:** Bar Chart (Vertical)

---

## 4. Xu hướng tin đăng theo thời gian
```sql
SELECT
  FROM_UNIXTIME(CAST(timestamp_iso/1000 AS BIGINT), 'yyyy-MM-dd') AS post_date,
  COUNT(*) AS job_count
FROM job_postings
GROUP BY post_date
ORDER BY post_date ASC;
```
**Visualization:** Line Chart (Time Series)

---

## 5. Phân phối mức lương trung bình theo ngành nghề
```sql
SELECT category, AVG(average_salary) AS avg_salary
FROM job_postings
WHERE category IS NOT NULL
GROUP BY category
ORDER BY avg_salary DESC;
```
**Visualization:** Bar Chart (Vertical)

---

## 6. Phân phối mức lương theo khu vực
```sql
SELECT region, AVG(average_salary) AS avg_salary
FROM job_postings
WHERE region IS NOT NULL
GROUP BY region
ORDER BY avg_salary DESC;
```
**Visualization:** Bar Chart (Vertical)

---

## 7. Tỷ lệ tin đăng theo bậc lương
```sql
SELECT salary_tier, COUNT(*) AS job_count
FROM job_postings
WHERE salary_tier IS NOT NULL
GROUP BY salary_tier
ORDER BY job_count DESC;
```
**Visualization:** Pie Chart

---

## 8. Top ngành nghề có mức lương trung bình cao nhất
```sql
SELECT category, AVG(average_salary) AS avg_salary
FROM job_postings
WHERE category IS NOT NULL
GROUP BY category
ORDER BY avg_salary DESC
LIMIT 10;
```
**Visualization:** Bar Chart (Horizontal)

---

## 9. Top công ty có mức lương trung bình cao nhất
```sql
SELECT company_name, AVG(average_salary) AS avg_salary
FROM job_postings
WHERE company_name IS NOT NULL
GROUP BY company_name
ORDER BY avg_salary DESC
LIMIT 10;
```
**Visualization:** Table (có thể kèm highlight giá trị cao)

---

## 10. Số lượng tin tuyển dụng theo ngày trong tuần
```sql
SELECT
  DAYOFWEEK(FROM_UNIXTIME(CAST(timestamp_iso/1000 AS BIGINT))) AS weekday,
  COUNT(*) AS job_count
FROM job_postings
GROUP BY weekday
ORDER BY weekday ASC;
```
**Visualization:** Line Chart hoặc Bar Chart (7 cột cho 7 ngày)

---
