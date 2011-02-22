sort = (values, arg) ->
 field = arg?.by
 reverse = arg?.order is 'desc'
 values.sort (a, b) ->
   if reverse then [a,b] = [b,a]
   if a?[field] < b?[field] then -1
   else if a?[field] is b?[field] then 0
   else if a?[field] > b?[field] then 1
