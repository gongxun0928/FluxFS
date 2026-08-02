//! Stable benchmark dimensions; real runners plug in client and UFS adapters.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupTemperature {
    NativeUfsHead,
    ExternalCold,
    ExternalWarm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupCase {
    pub temperature: LookupTemperature,
    pub path_depth: u8,
    pub directory_entries: u32,
}

/// Initial matrix for apples-to-apples native UFS vs FluxFS lookup results.
///
/// Report latency distribution and request amplification for every case. Do
/// not turn these placeholders into an SLO until hardware, backend, payload,
/// cache state, and concurrency are recorded with the result.
pub fn lookup_matrix() -> Vec<LookupCase> {
    const DEPTHS: [u8; 3] = [1, 4, 16];
    const DIRECTORY_SIZES: [u32; 3] = [1, 1_000, 1_000_000];
    const TEMPERATURES: [LookupTemperature; 3] = [
        LookupTemperature::NativeUfsHead,
        LookupTemperature::ExternalCold,
        LookupTemperature::ExternalWarm,
    ];

    TEMPERATURES
        .into_iter()
        .flat_map(|temperature| {
            DEPTHS.into_iter().flat_map(move |path_depth| {
                DIRECTORY_SIZES
                    .into_iter()
                    .map(move |directory_entries| LookupCase {
                        temperature,
                        path_depth,
                        directory_entries,
                    })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_covers_each_dimension() {
        let cases = lookup_matrix();
        assert_eq!(cases.len(), 27);
        assert!(cases.iter().any(|case| {
            case.temperature == LookupTemperature::ExternalCold
                && case.path_depth == 16
                && case.directory_entries == 1_000_000
        }));
    }
}
