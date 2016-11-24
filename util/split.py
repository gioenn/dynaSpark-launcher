import math

number = 35
n = 10

values = []
while (number > 0 and n > 0):
    a = math.floor(number / n)
    number -= a
    n -= 1
    values.append(a)

print(values)
print(sum(values))
