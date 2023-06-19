from Tool_Pack import tools


a = "flying fish flew by the space station"
b = "we will not allow you to bring your pet armadillo along"
c = "he figured a few sticks of dynamite were easier than a fishing pole to catch fish"
k = 2


def shingle(text: str, k: int):
    shingle_set = []
    for i in range(len(text) - k+1):
        shingle_set.append(text[i:i+k])
    return set(shingle_set)


if __name__ == "__main__":
    a = shingle(a, k)
    b = shingle(b, k)
    c = shingle(c, k)
    print(a)
