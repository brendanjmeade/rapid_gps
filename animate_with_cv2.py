import cv2
import glob
import os


FILE_PATH = 'E:/Pictures/2015_Slideshow(limited_normalized_week)'

img_list = []
print('Beginning to Glob')
# The glob is sorted based on the time of creation
file_names = sorted((glob.glob(FILE_PATH +'/*.png')), key=os.path.getmtime)
print('No Longer globbing')

for filename in file_names:
    img = cv2.imread(filename)
    height, width, layers = img.shape
    size = (width, height)
    img_list.append(img)


out = cv2.VideoWriter('E:/Pictures/mp4s_of_project/2015_week_1.mp4',cv2.VideoWriter_fourcc(*'mp4v'), 30, size)

for i in range(len(img_list)):
    out.write(img_list[i])
out.release()
