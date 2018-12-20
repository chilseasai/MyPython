# —*— coding: utf-8 -*-
def move(n, a, b, c):
    if n == 1:
        print(a, '-->', c)
        return
    move(n-1, a, c, b)
    print(a, '-->', c)
    move(n-1, b, a, c)


def test():
    print('hello world')

if __name__ == '__main__':
    move(4, 'A', 'B', 'C')
