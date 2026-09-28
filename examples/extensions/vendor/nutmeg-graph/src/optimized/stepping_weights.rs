//! Capture weights from the very same staged revision as the cached projection.
use super::*;

impl Store {
    pub(crate) fn stepping_projection(
        &self,
        name: &str,
        args: &ValidatedArguments,
        context: &ExecutionContext,
    ) -> Result<(GraphProjection, Vec<f64>, MemoryReservation)> {
        let entry = self.existing(name)?;
        let graph = self.projection(name, args)?;
        // Drop/restage may create another Entry whose revision counter also
        // starts at one. Revision equality alone would not pin the snapshot.
        if !Arc::ptr_eq(&entry, &self.existing(name)?) {
            return exec_err!(
                "nutmeg: graph replaced while capturing traversal weights; retry the read"
            );
        }
        let staged = entry.read().map_err(|_| poisoned())?;
        if graph.identity().revision() != format!("r{}", staged.revision) {
            return exec_err!(
                "nutmeg: graph changed while capturing traversal weights; retry the read"
            );
        }
        let key = match args.options().get("weightProperty") {
            Some(Value::String(key)) => key,
            _ => return plan_err!("ssspDeltaStar requires a DOUBLE weightProperty"),
        };
        let reservation = context
            .reserve(graph.edge_count().saturating_mul(8).saturating_add(4096))
            .map_err(err)?;
        let mut weights = Vec::new();
        weights.try_reserve_exact(graph.edge_count()).map_err(err)?;
        let property_name = format!("property.{key}");
        let presence_name = format!("present.{key}");
        let mut offset = 0;
        let mut selected = graph.edges().iter().peekable();
        for batch in &staged.edges {
            let end = offset + batch.num_rows();
            if selected.peek().is_some_and(|edge| edge.ordinal < end) {
                let column = batch
                    .column_by_name(&property_name)
                    .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                    .ok_or_else(|| err("ssspDeltaStar weightProperty must be DOUBLE"))?;
                let present = batch
                    .column_by_name(&presence_name)
                    .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                    .ok_or_else(|| err("missing weight presence column"))?;
                while selected.peek().is_some_and(|edge| edge.ordinal < end) {
                    context.charge_work(1).map_err(err)?;
                    let edge = selected
                        .next()
                        .ok_or_else(|| err("missing selected edge"))?;
                    let row = edge
                        .ordinal
                        .checked_sub(offset)
                        .ok_or_else(|| err("projection edge order changed"))?;
                    if column.is_null(row) || present.is_null(row) || !present.value(row) {
                        return exec_err!(
                            "ssspDeltaStar requires a non-null weight on every selected edge"
                        );
                    }
                    let value = column.value(row);
                    if !value.is_finite() || value < 0.0 {
                        return exec_err!("SSSP weights must be finite and nonnegative");
                    }
                    weights.push(if value == 0.0 { 0.0 } else { value });
                }
            }
            offset = end;
        }
        if selected.next().is_some() {
            return exec_err!("projection ordinal outside staged edges");
        }
        Ok((graph, weights, reservation))
    }
}
