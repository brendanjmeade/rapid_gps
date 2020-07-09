import cv2
import glob
# import numpy as np

FILE_PATH = 'E:/Pictures/2015 Slideshow(set limits)'

img_array = []
print('Beginning to Glob')
file_names = glob.glob(FILE_PATH +'/*.png', recursive=True)
print('No Longer globbing')

for filename in file_names:
    img = cv2.imread(filename)
    height, width, layers = img.shape
    size = (width, height)
    img_array.append(img)

out = cv2.VideoWriter('project.mp4',cv2.VideoWriter_fourcc(*'mp4v'), 5, size)

for i in range(len(img_array)):
    out.write(img_array[i])
out.release()