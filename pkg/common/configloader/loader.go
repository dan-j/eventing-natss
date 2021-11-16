package configloader

type Loader func(path string) (map[string]string, error)
