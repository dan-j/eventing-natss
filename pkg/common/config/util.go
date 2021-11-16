package config

// JoinStringMaps returns a new map containing the contents of both argument maps, preferring the contents of map1 on conflict.
func JoinStringMaps(map1 map[string]string, map2 map[string]string) map[string]string {
	resultMap := make(map[string]string, len(map1))
	for map1Key, map1Value := range map1 {
		resultMap[map1Key] = map1Value
	}
	for map2Key, map2Value := range map2 {
		if _, ok := map1[map2Key]; !ok {
			resultMap[map2Key] = map2Value
		}
	}
	return resultMap
}
