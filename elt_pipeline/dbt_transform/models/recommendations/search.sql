select
  isbn,
  name
from {{ source('gold', 'book_with_info') }}
