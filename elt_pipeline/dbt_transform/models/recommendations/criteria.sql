with tmp_avg_rating as (
  select
    isbn,
    rating
  from {{ source('gold', 'book_with_rating') }}
),
tmp_download_link as (
  select
    isbn,
    case
      when link is null then 0
      else 1
    end as hasdownloadlink,
    rating
  from {{ source('recommendations', 'book_download_link') }}
  right join tmp_avg_rating using (isbn)
)

select *
from tmp_download_link
