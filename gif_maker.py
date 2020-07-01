import PIL.Image as PIL
import numpy as np

image_frames = []

days = np.arange(1, 15)

for k in days:
    new_frame = PIL.open(r'C:/Users/M1/Pictures/SCEC IMAGES/Slide Show' + '/Figure_' + str(k) + '.png')
    image_frames.append(new_frame)

# Saves to working directory
image_frames[0].save('movement_timelapse.gif', format= 'GIF', append_images=image_frames[1:],
                     save_all=True, duration=800, loop=0)  # set as an infinite loop
