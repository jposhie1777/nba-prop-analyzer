export default function PropsTestScreen() {
  const { colors } = useTheme();
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const {
    props,
    loading,
    filters,
    setFilters,
  } = usePlayerPropsMaster();

  const listRef = useRef<FlatList>(null);

  /* ============================
     DEBUG VISIBILITY
  ============================ */
  useEffect(() => {
    console.log("📺 [SCREEN] props length:", props.length);
    if (props.length > 0) {
      console.log("📺 [SCREEN] first prop:", props[0]);
    }
  }, [props]);

  if (loading) {
    return (
      <View style={styles.center}>
        <Text style={styles.loading}>Loading test props…</Text>
      </View>
    );
  }

  return (
    <View style={styles.screen}>
      <Text style={{ padding: 12, color: "red", fontWeight: "700" }}>
        PROPS COUNT: {props.length}
      </Text>

      <FlatList
        ref={listRef}
        data={props}
        keyExtractor={(item) => item.id}
        renderItem={({ item }) => (
          <PropCard
            {...item}
            hitPct={
              item.hitRate != null
                ? Math.round(item.hitRate * 100)
                : 0
            }
          />
        )}
        showsVerticalScrollIndicator={false}
      />
    </View>
  );
}