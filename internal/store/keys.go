package store

import "fmt"

// Keys use Redis hash tags kvfs:{ns}:... so MULTI/EXEC stays on one cluster slot.
func (s *Store) counterKey() string {
	return fmt.Sprintf("kvfs:{%s}:counter", s.ns)
}

func (s *Store) metaKey(parentID uint64, name string) string {
	return fmt.Sprintf("kvfs:{%s}:meta:%d:%s", s.ns, parentID, name)
}

func (s *Store) dirKey(parentID uint64, name string) string {
	return fmt.Sprintf("kvfs:{%s}:dir:%d:%s", s.ns, parentID, name)
}

func (s *Store) dataKey(parentID uint64, name string) string {
	return fmt.Sprintf("kvfs:{%s}:data:%d:%s", s.ns, parentID, name)
}

func (s *Store) Channel() string {
	return fmt.Sprintf("kvfs:{%s}", s.ns)
}

func InvalidatePayload(parentID uint64, name string) string {
	return fmt.Sprintf("%d:%s", parentID, name)
}
