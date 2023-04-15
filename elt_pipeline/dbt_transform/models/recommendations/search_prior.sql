select
  isbn,
  name
from {{ source('gold', 'book_with_info') }}
right join {{ source('recommendations', 'book_download_link') }} using (isbn)
where link is not null
