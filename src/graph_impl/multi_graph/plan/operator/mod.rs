/// Get common attributes(mutable) from EI Operator
#[macro_export]
macro_rules! get_ei_as_mut {
    ($item:expr) => {
        match $item {
            EI::Base(base)=>base,
            EI::Extend(base)=> &mut base.base_ei,
            EI::Intersect(base)=> &mut base.base_ei,
        }
    };
}

#[macro_export]
macro_rules! get_ei_as_ref {
    ($item:expr) => {
        match $item {
            EI::Base(base)=>base,
            EI::Extend(base)=> &base.base_ei,
            EI::Intersect(base)=> &base.base_ei,
        }
    };
}

#[macro_export]
macro_rules! get_scan_as_mut {
    ($item:expr) => {
        match $item {
            Scan::Base(base)=>base,
            Scan::ScanSampling(base)=> &mut base.base_scan,
            Scan::ScanBlocking(base)=> &mut base.base_scan,
        }
    };
}

#[macro_export]
macro_rules! get_scan_as_ref {
    ($item:expr) => {
        match $item {
            Scan::Base(base)=>base,
            Scan::ScanSampling(base)=> &base.base_scan,
            Scan::ScanBlocking(base)=> &base.base_scan,
        }
    };
}

/// Get common attributes(Origin) from Operator
#[macro_export]
macro_rules! get_op_attr {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => base.$name,
            Operator::Sink(sink) => sink.base_op.$name,
            Operator::Scan(scan) => match scan{
                Scan::Base(base)=>base.base_op.$name,
                Scan::ScanSampling(base)=>base.base_scan.base_op.$name,
                Scan::ScanBlocking(base)=>base.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base)=>base.base_op.$name,
                EI::Extend(base)=>base.base_ei.base_op.$name,
                EI::Intersect(base)=>base.base_ei.base_op.$name,
            },
        }
    };
}

/// Get common attributes(reference) from Operator
#[macro_export]
macro_rules! get_op_attr_as_ref {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => &base.$name,
            Operator::Sink(sink) => &sink.base_op.$name,
            Operator::Scan(scan) => match scan{
                Scan::Base(base)=>&base.base_op.$name,
                Scan::ScanSampling(base)=>&base.base_scan.base_op.$name,
                Scan::ScanBlocking(base)=>&base.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base)=>&base.base_op.$name,
                EI::Extend(base)=>&base.base_ei.base_op.$name,
                EI::Intersect(base)=>&base.base_ei.base_op.$name,
            },
        }
    };
}

/// Get common attributes(mutable) from Operator
#[macro_export]
macro_rules! get_op_attr_as_mut {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => &mut base.$name,
            Operator::Sink(sink) => &mut sink.base_op.$name,
            Operator::Scan(scan) => match scan{
                Scan::Base(base)=>&mut base.base_op.$name,
                Scan::ScanSampling(base)=>&mut base.base_scan.base_op.$name,
                Scan::ScanBlocking(base)=>&mut base.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base)=>&mut base.base_op.$name,
                EI::Extend(base)=>&mut base.base_ei.base_op.$name,
                EI::Intersect(base)=>&mut base.base_ei.base_op.$name,
            },
        }
    };
}
pub mod operator;
pub mod scan;
pub mod sink;
pub mod extend;