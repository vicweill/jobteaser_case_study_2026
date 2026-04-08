/** Test if the application rate is always between 0 and 1 **/

select
    month,
    application_rate
from {{ ref('application_rate') }}
where application_rate < 0 or application_rate > 1
