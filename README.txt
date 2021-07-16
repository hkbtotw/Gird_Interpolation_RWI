# Script to compute interpolation of facebook relative wealth index of Thailand to fit on H3's Lv8 grids

Update:
2021-07-16  Created by Tawan,T  , rwi data from facebook updaed on 2021-04-21

Why:
- Facebook relative wealth index is an indicator of living standard in Thailand
- Data comes in point ( lat lng) covering 2.5 km2 area throughout Thailand
- Need to interpolate the rwi onto Lv8 H3'grid in order to process and analyze with current analytical framework

How:
1.python Grid_Interpolation_Lv7_Lv8_H3Grid.python

Output data:
1.Write on database H3_Grid_RWI_Lv8_Province