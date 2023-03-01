def pipe(data, *functions):
  for function in functions:
    data = function(data)

  return data

def nullable_pipe(data, *functions):
  if data is None:
    return None

  for function in functions:
    data = function(data)

  return data