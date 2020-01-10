// TODO: Rust strings don't have SSO.
//  Depending on the average string length SSO might perform better due to less heap allocations.
pub type Key = String;
pub type Val = String;
