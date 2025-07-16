import os
import geopandas as gpd
import pandas as pd
import numpy as np
import osmnx as ox
import networkx as nx
import parmap
from multiprocessing import Pool, cpu_count, Manager



# 네트워크 거리 측정 함수
def calculate_distance(idx_1, idx_2, stops, G):
    try:
        orig_node = ox.distance.nearest_nodes(G, stops.at[idx_1, 'stop_lon'], stops.at[idx_1, 'stop_lat'])
        dest_node = ox.distance.nearest_nodes(G, stops.at[idx_2, 'stop_lon'], stops.at[idx_2, 'stop_lat'])

        shortest_path = ox.routing.shortest_path(G, orig_node, dest_node, weight='length', cpus=1)
        if shortest_path is None:
            return None

        edge_length = np.array([G[shortest_path[i]][shortest_path[i + 1]][0]['length'] for i in range(len(shortest_path) - 1)])
        network_distance = np.sum(edge_length)

        orig_point = (stops.at[idx_1, 'stop_lon'], stops.at[idx_1, 'stop_lat'])
        orig_node_point = (G.nodes[orig_node]['x'], G.nodes[orig_node]['y'])
        dest_point = (stops.at[idx_2, 'stop_lon'], stops.at[idx_2, 'stop_lat'])
        dest_node_point = (G.nodes[dest_node]['x'], G.nodes[dest_node]['y'])

        direct_distance_orig = ox.distance.great_circle(orig_point[0], orig_point[1], orig_node_point[0], orig_node_point[1])
        direct_distance_dest = ox.distance.great_circle(dest_point[0], dest_point[1], dest_node_point[0], dest_node_point[1])

        distance = network_distance + direct_distance_dest + direct_distance_orig

        # 거리 300미터 미만이면 None
        if distance < 300:
            return None

        # 거리 300미터 이상일 때만 기록
        result = {
            'ORIG_ID': stops.at[idx_1, 'stop_id'],
            'DEST_ID': stops.at[idx_2, 'stop_id'],
            'DISTANCE': distance
        }

        return result

    except Exception as e:
        return None


# 특정 ADM_NM에 대해 필터링된 데이터프레임 생성
def process_adm(adm_name, df_stops, g1):
    target_stops = df_stops[df_stops['ADM_NM'] == adm_name]
    pairs = []

    target_indices = list(target_stops.index)
    df_stops_indices = list(df_stops.index)

    for idx_1 in target_indices:
        for idx_2 in df_stops_indices:
            # print(idx_1, idx_2)
            if df_stops.at[idx_1, 'TOT_REG_CD'] == df_stops.at[idx_2, 'TOT_REG_CD']:
                print(idx_1, idx_2, "continue")
                continue
            result = calculate_distance(idx_1, idx_2, df_stops, g1) # 거리 300미터 미만이면 None
            if result: # result가 None이 아닐 경우
                # print(result)
                pairs.append(result)

    return pairs

# 멀티프로세싱 설정
if __name__ == "__main__":
    
    os.chdir(f'D:/desktop/graduation/gtfs2022_sibal')

    # 정류장 파일 로드
    stops = pd.read_csv('stops.txt',
                           encoding='utf-8')
    print("2022년 정류장 파일 로드 완료")

    g_stops = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat), crs='EPSG:4326')
    print("2022년 정류장 Geodataframe 생성 완료")

    # 정류장 범위에 맞게 그래프 추출
    lat_max = g_stops['stop_lat'].max()
    lat_min = g_stops['stop_lat'].min()
    lon_max = g_stops['stop_lon'].max()
    lon_min = g_stops['stop_lon'].min()

    bbox = lon_min, lat_min, lon_max, lat_max

    g1 = ox.graph_from_bbox(bbox, network_type='all'
                                , custom_filter='["highway"]'
                                )

    print("2022년 그래프 생성 완료")

    os.chdir('D:/desktop/graduation/')

    # try:
    #     bus_oa = gpd.read_file("bus_oa_2022.shp", encoding = 'cp949')
    #     print("2022년 정류장 shp파일 cp949 인코딩으로 로드 완료")
    # except:
    bus_oa = gpd.read_file(f"bus_oa_2022.shp", encoding = 'utf-8')
    print("2022년 정류장 shp파일 utf-8 인코딩으로 로드 완료")

        
    df_stops = stops.merge(bus_oa[["stop_id", "ADM_CD", "ADM_NM", "TOT_REG_CD"]], on='stop_id', how = 'left')

    print("2022년 정류장 merge 완료")
    
    target_adm = ["불당1동", "불당2동", "부성1동", "부성2동"]

        

    cpu_pool_count = cpu_count() // 2

    print('2022년 멀티프로세싱 시작')
        
    with Pool(cpu_pool_count) as pool:
        results = pool.starmap(process_adm, [(adm, df_stops, g1) for adm in target_adm]) # parmap을 사용하여 진행률 표시
            
        
    for adm_name, adm_result in zip(target_adm, results):
        df_adm = pd.DataFrame(adm_result)
        df_adm.to_csv(f'distance_results_2022_{adm_name}.csv', encoding='utf-8', index=False)
                
        print(f"2022년 {adm_name} 데이터 CSV 저장 완료!")
