use differential_datalog::debug::*;
use differential_datalog::program::*;
use differential_datalog::record;
use differential_datalog::record::IntoRecord;

use libc::size_t;
use std::ffi;
use std::fs;
use std::io;
use std::iter;
use std::mem;
use std::os::raw;
use std::os::unix;
use std::os::unix::io::FromRawFd;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};

use super::update_handler::*;
use super::valmap::*;
use super::*;

/* FlatBuffers bindings generated by `ddlog` */
#[cfg(feature = "flatbuf")]
use flatbuf;

#[cfg(feature = "flatbuf")]
use flatbuf::FromFlatBuffer;

#[derive(Debug)]
pub struct HDDlog {
    pub prog: Mutex<RunningProgram<Value>>,
    pub update_handler: Box<dyn IMTUpdateHandler<Value>>,
    pub db: Option<Arc<Mutex<DeltaMap>>>,
    pub deltadb: Arc<Mutex<Option<DeltaMap>>>,
    pub print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    /// When set, all commands sent to the program are recorded in
    /// the specified `.dat` file so that they can be replayed later. */
    pub replay_file: Option<Mutex<fs::File>>,
    /// Debugger currently attached to the program or `None` if there is no debugger currently
    /// attached or if the program was instantiated with debugging disabled.
    pub debugger: Option<Box<Debugger>>,
    /// `AtomicPtr` that stores pointer to the currently connected debugger.  This field is `None`
    /// if the program was instantiated with debugging hooks disabled.  If, however, debugging
    /// hooks are enabled but there is no debugger connected to the program at the moment, this
    /// field will contain `Some<AtomicPtr<null>>`.
    pub debugger_ptr: Option<Box<AtomicPtr<Debugger>>>,
}

/* Public API */
impl HDDlog {
    pub fn print_err(f: Option<extern "C" fn(msg: *const raw::c_char)>, msg: &str) {
        match f {
            None => eprintln!("{}", msg),
            Some(f) => f(ffi::CString::new(msg).unwrap().into_raw()),
        }
    }

    pub fn eprintln(&self, msg: &str) {
        Self::print_err(self.print_err, msg)
    }

    pub fn get_table_id(tname: &str) -> Result<Relations, String> {
        relname2id(tname).ok_or_else(|| format!("unknown relation {}", tname))
    }

    pub fn get_table_name(tid: RelId) -> Result<&'static str, String> {
        relid2name(tid).ok_or_else(|| format!("unknown relation {}", tid))
    }

    pub fn get_table_cname(tid: RelId) -> Result<&'static ffi::CStr, String> {
        relid2cname(tid).ok_or_else(|| format!("unknown relation {}", tid))
    }

    pub fn run<F>(workers: usize, do_store: bool, cb: F, debug: bool) -> HDDlog
    where
        F: Callback,
    {
        Self::do_run(
            workers,
            do_store,
            CallbackUpdateHandler::new(cb),
            None,
            if debug { Some(None) } else { None },
        )
    }

    pub fn run_with_debugger<F>(workers: usize, do_store: bool, cb: F, debugger: Debugger) -> HDDlog
    where
        F: Callback,
    {
        Self::do_run(
            workers,
            do_store,
            CallbackUpdateHandler::new(cb),
            None,
            Some(Some(Box::new(debugger))),
        )
    }

    pub fn attach_debugger(&mut self, debugger: Debugger) -> Result<(), String> {
        self.do_attach_debugger(Box::new(debugger))
    }

    fn do_attach_debugger(&mut self, mut debugger: Box<Debugger>) -> Result<(), String> {
        let prog = self.prog.lock().unwrap();
        /* We have a lock on `self.prog`.  No other methods, including `commit`, can be running
         * now, so the debugger is not being accessed and it's safe to replace it.
         */
        if let Some(ref debugger_ptr) = self.debugger_ptr {
            debugger_ptr.store(&mut *debugger as *mut Debugger, Ordering::SeqCst);
            self.debugger = Some(debugger);
            Ok(())
        } else {
            Err(
                "Cannot attach debugger: the program was started with debugging hooks disabled"
                    .to_string(),
            )
        }
    }

    pub fn detach_debugger(&mut self) {
        let prog = self.prog.lock().unwrap();
        /* We have a lock on `self.prog`.  No other methods, including `commit`, can be running
         * now, so the debugger is not being accessed and it's safe to deallocate it.
         */
        if let Some(ref debugger_ptr) = self.debugger_ptr {
            self.debugger = None;
            debugger_ptr.store(ptr::null_mut(), Ordering::SeqCst);
        }
    }

    pub fn record_commands(&mut self, file: &mut Option<Mutex<fs::File>>) {
        mem::swap(&mut self.replay_file, file);
    }

    pub fn dump_input_snapshot<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        for (rel, relname) in INPUT_RELIDMAP.iter() {
            let prog = self.prog.lock().unwrap();
            match prog.get_input_relation_data(*rel as RelId) {
                Ok(valset) => {
                    for v in valset.iter() {
                        writeln!(w, "insert {}[{}],", relname, v)?;
                    }
                }
                _ => match prog.get_input_relation_index(*rel as RelId) {
                    Ok(ivalset) => {
                        for v in ivalset.values() {
                            writeln!(w, "insert {}[{}],", relname, v)?;
                        }
                    }
                    _ => {
                        panic!("Unknown input relation {:?} in dump_input_snapshot", rel);
                    }
                },
            }
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.prog.lock().unwrap().stop()
    }

    pub fn transaction_start(&self) -> Result<(), String> {
        self.record_transaction_start();
        self.prog.lock().unwrap().transaction_start()
    }

    pub fn transaction_commit_dump_changes(&self) -> Result<DeltaMap, String> {
        self.record_transaction_commit(true);
        *self.deltadb.lock().unwrap() = Some(DeltaMap::new());

        self.update_handler.before_commit();
        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                let mut delta = self.deltadb.lock().unwrap();
                Ok(delta.take().unwrap())
            }
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    pub fn transaction_commit(&self) -> Result<(), String> {
        self.record_transaction_commit(false);
        self.update_handler.before_commit();

        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                Ok(())
            }
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    pub fn transaction_rollback(&self) -> Result<(), String> {
        let _ = self.record_transaction_rollback();
        self.prog.lock().unwrap().transaction_rollback()
    }

    /* Two implementations of `apply_updates`: one that takes `Record`s and one that takes `Value`s.
     */
    pub fn apply_updates<V, I>(&self, upds: I) -> Result<(), String>
    where
        V: Deref<Target = record::UpdCmd>,
        I: iter::Iterator<Item = V>,
    {
        let mut conversion_err = false;
        let mut msg: Option<String> = None;

        /* Iterate through all updates, but only feed them to `apply_valupdates` until we reach
         * the first invalid command.
         * XXX: We must iterate till the end of `upds`, as `ddlog_apply_updates` relies on this to
         * deallocate all commands.
         */
        let res = self.apply_valupdates(upds.flat_map(|u| {
            if conversion_err {
                None
            } else {
                match updcmd2upd(u.deref()) {
                    Ok(u) => Some(u),
                    Err(e) => {
                        conversion_err = true;
                        msg = Some(format!("invalid command {:?}: {}", *u, e));
                        None
                    }
                }
            }
        }));
        match msg {
            Some(e) => Err(e),
            None => res,
        }
    }

    #[cfg(feature = "flatbuf")]
    pub fn apply_updates_from_flatbuf(&self, buf: &[u8]) -> Result<(), String> {
        let cmditer = flatbuf::updates_from_flatbuf(buf)?;
        let upds: Result<Vec<Update<Value>>, String> =
            cmditer.map(|cmd| Update::from_flatbuf(cmd)).collect();
        self.apply_valupdates(upds?.into_iter())
    }

    pub fn apply_valupdates<I: iter::Iterator<Item = Update<Value>>>(
        &self,
        upds: I,
    ) -> Result<(), String> {
        if let Some(ref f) = self.replay_file {
            let mut file = f.lock().unwrap();
            /* Count the number of elements in `upds`. */
            let mut n = 0;

            let res = self
                .prog
                .lock()
                .unwrap()
                .apply_updates(upds.enumerate().map(|(i, upd)| {
                    n += 1;
                    if i > 0 {
                        let _ = writeln!(file, ",");
                    };
                    record_valupdate(&mut *file, &upd);
                    upd
                }));
            /* Print semicolon if `upds` were not empty. */
            if n > 0 {
                let _ = writeln!(file, ";");
            }
            res
        } else {
            self.prog.lock().unwrap().apply_updates(upds)
        }
    }

    pub fn clear_relation(&self, table: usize) -> Result<(), String> {
        self.record_clear_relation(table);
        self.prog.lock().unwrap().clear_relation(table)
    }

    pub fn dump_table<F>(&self, table: usize, cb: Option<F>) -> Result<(), &'static str>
    where
        F: Fn(&record::Record) -> bool,
    {
        self.record_dump_table(table);
        if let Some(ref db) = self.db {
            HDDlog::db_dump_table(&mut db.lock().unwrap(), table, cb);
            Ok(())
        } else {
            Err("cannot dump table: ddlog_run() was invoked with do_store flag set to false")
        }
    }

    /*
     * Controls recording of differential operator runtimes.  When enabled,
     * DDlog records each activation of every operator and prints the
     * per-operator CPU usage summary in the profile.  When disabled, the
     * recording stops, but the previously accumulated profile is preserved.
     *
     * Recording CPU events can be expensive in large dataflows and is
     * therefore disabled by default.
     */
    pub fn enable_cpu_profiling(&self, enable: bool) {
        self.record_enable_cpu_profiling(enable);
        self.prog.lock().unwrap().enable_cpu_profiling(enable);
    }

    /*
     * returns DDlog program runtime profile
     */
    pub fn profile(&self) -> String {
        self.record_profile();
        let rprog = self.prog.lock().unwrap();
        let profile: String = rprog.profile.lock().unwrap().to_string();
        profile
    }
}

/* Internals */
impl HDDlog {
    fn do_run<UH>(
        workers: usize,
        do_store: bool,
        cb: UH,
        print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
        debugger: Option<Option<Box<Debugger>>>,
    ) -> HDDlog
    where
        UH: UpdateHandler<Value> + Send + 'static,
    {
        let workers = if workers == 0 { 1 } else { workers };

        let db: Arc<Mutex<DeltaMap>> = Arc::new(Mutex::new(DeltaMap::new()));
        let db2 = db.clone();

        let deltadb: Arc<Mutex<Option<DeltaMap>>> = Arc::new(Mutex::new(None));
        let deltadb2 = deltadb.clone();

        let handler: Box<dyn IMTUpdateHandler<Value>> = {
            let handler_generator = move || {
                /* Always use delta handler, which costs nothing unless it is
                 * actually used*/
                let delta_handler = DeltaUpdateHandler::new(deltadb2);

                let store_handler = if do_store {
                    Some(ValMapUpdateHandler::new(db2))
                } else {
                    None
                };

                let cb_handler = Box::new(cb) as Box<dyn UpdateHandler<Value> + Send>;
                let mut handlers: Vec<Box<dyn UpdateHandler<Value>>> = Vec::new();
                handlers.push(Box::new(delta_handler));
                if let Some(h) = store_handler {
                    handlers.push(Box::new(h))
                };
                handlers.push(cb_handler);
                Box::new(ChainedUpdateHandler::new(handlers)) as Box<dyn UpdateHandler<Value>>
            };
            Box::new(ThreadUpdateHandler::new(handler_generator))
        };

        let program = prog(handler.mt_update_cb(), debugger.is_some());

        /* Notify handler about initial transaction */
        handler.before_commit();
        let (debugger, debugger_ptr, debugger_raw_ptr) = match debugger {
            // Debugging disabled.
            None => (None, None, ptr::null()),
            // Debugging hooks enabled, but no debugger connected at startup.
            Some(None) => {
                let ptr = Box::new(AtomicPtr::new(ptr::null_mut()));
                let raw_ptr = &*ptr as *const AtomicPtr<Debugger>;
                (None, Some(ptr), raw_ptr)
            }
            // Debugging hooks enabled; started the program with debugger connected.
            Some(Some(mut d)) => {
                let ptr = Box::new(AtomicPtr::new(&mut *d as *mut Debugger));
                let raw_ptr = &*ptr as *const AtomicPtr<Debugger>;
                (Some(d), Some(ptr), raw_ptr)
            }
        };
        let prog = program.run(workers as usize, debugger_raw_ptr);
        handler.after_commit(true);

        HDDlog {
            prog: Mutex::new(prog),
            update_handler: handler,
            db: Some(db),
            deltadb,
            print_err,
            replay_file: None,
            debugger,
            debugger_ptr,
        }
    }

    fn db_dump_table<F>(db: &mut DeltaMap, table: libc::size_t, cb: Option<F>)
    where
        F: Fn(&record::Record) -> bool,
    {
        if let Some(f) = cb {
            for (val, w) in db.get_rel(table) {
                assert!(*w == 1);
                if !f(&val.clone().into_record()) {
                    break;
                }
            }
        };
    }

    fn record_transaction_start(&self) {
        if let Some(ref f) = self.replay_file {
            if writeln!(f.lock().unwrap(), "start;").is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }

    fn record_transaction_commit(&self, record_changes: bool) {
        if let Some(ref f) = self.replay_file {
            let res = if record_changes {
                writeln!(f.lock().unwrap(), "commit dump_changes;")
            } else {
                writeln!(f.lock().unwrap(), "commit;")
            };
            if res.is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }

    fn record_transaction_rollback(&self) -> Result<(), String> {
        if let Some(ref f) = self.replay_file {
            if writeln!(f.lock().unwrap(), "rollback;").is_err() {
                Err("failed to record invocation in replay file".to_string())
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    fn record_clear_relation(&self, table: usize) {
        if let Some(ref f) = self.replay_file {
            if writeln!(
                f.lock().unwrap(),
                "clear {};",
                relid2name(table).unwrap_or(&"???")
            )
            .is_err()
            {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }

    fn record_dump_table(&self, table: usize) {
        if let Some(ref f) = self.replay_file {
            if writeln!(
                f.lock().unwrap(),
                "dump {};",
                relid2name(table).unwrap_or(&"???")
            )
            .is_err()
            {
                self.eprintln("ddlog_dump_table(): failed to record invocation in replay file");
            }
        }
    }

    fn record_enable_cpu_profiling(&self, enable: bool) {
        if let Some(ref f) = self.replay_file {
            if writeln!(
                f.lock().unwrap(),
                "profile cpu {};",
                if enable { "on" } else { "off" }
            )
            .is_err()
            {
                self.eprintln(
                    "ddlog_cpu_profiling_enable(): failed to record invocation in replay file",
                );
            }
        }
    }

    fn record_profile(&self) {
        if let Some(ref f) = self.replay_file {
            if writeln!(f.lock().unwrap(), "profile;").is_err() {
                self.eprintln("failed to record invocation in replay file");
            }
        }
    }
}

pub fn record_update(file: &mut fs::File, upd: &record::UpdCmd) {
    match upd {
        record::UpdCmd::Insert(rel, record) => {
            let _ = write!(
                file,
                "insert {}[{}]",
                relident2name(rel).unwrap_or(&"???"),
                record
            );
        }
        record::UpdCmd::Delete(rel, record) => {
            let _ = write!(
                file,
                "delete {}[{}]",
                relident2name(rel).unwrap_or(&"???"),
                record
            );
        }
        record::UpdCmd::DeleteKey(rel, record) => {
            let _ = write!(
                file,
                "delete_key {} {}",
                relident2name(rel).unwrap_or(&"???"),
                record
            );
        }
        record::UpdCmd::Modify(rel, key, mutator) => {
            let _ = write!(
                file,
                "modify {} {} <- {}",
                relident2name(rel).unwrap_or(&"???"),
                key,
                mutator
            );
        }
    }
}

pub fn record_valupdate(file: &mut fs::File, upd: &Update<Value>) {
    match upd {
        Update::Insert { relid, v } => {
            let _ = write!(
                file,
                "insert {}[{}]",
                relid2name(*relid).unwrap_or(&"???"),
                v
            );
        }
        Update::DeleteValue { relid, v } => {
            let _ = write!(
                file,
                "delete {}[{}]",
                relid2name(*relid).unwrap_or(&"???"),
                v
            );
        }
        Update::DeleteKey { relid, k } => {
            let _ = write!(
                file,
                "delete_key {} {}",
                relid2name(*relid).unwrap_or(&"???"),
                k
            );
        }
        Update::Modify { relid, k, m } => {
            let _ = write!(
                file,
                "modify {} {} <- {}",
                relid2name(*relid).unwrap_or(&"???"),
                k,
                m
            );
        }
    }
}

pub fn updcmd2upd(c: &record::UpdCmd) -> Result<Update<Value>, String> {
    match c {
        record::UpdCmd::Insert(rident, rec) => {
            let relid: Relations =
                relident2id(rident).ok_or_else(|| format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Insert {
                relid: relid as RelId,
                v: val,
            })
        }
        record::UpdCmd::Delete(rident, rec) => {
            let relid: Relations =
                relident2id(rident).ok_or_else(|| format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::DeleteValue {
                relid: relid as RelId,
                v: val,
            })
        }
        record::UpdCmd::DeleteKey(rident, rec) => {
            let relid: Relations =
                relident2id(rident).ok_or_else(|| format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, rec)?;
            Ok(Update::DeleteKey {
                relid: relid as RelId,
                k: key,
            })
        }
        record::UpdCmd::Modify(rident, key, rec) => {
            let relid: Relations =
                relident2id(rident).ok_or_else(|| format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, key)?;
            Ok(Update::Modify {
                relid: relid as RelId,
                k: key,
                m: Box::new(rec.clone()),
            })
        }
    }
}

fn relident2id(r: &record::RelIdentifier) -> Option<Relations> {
    match r {
        record::RelIdentifier::RelName(rname) => relname2id(rname),
        record::RelIdentifier::RelId(id) => relid2rel(*id),
    }
}

fn relident2name(r: &record::RelIdentifier) -> Option<&str> {
    match r {
        record::RelIdentifier::RelName(rname) => Some(rname.as_ref()),
        record::RelIdentifier::RelId(id) => relid2name(*id),
    }
}

/***************************************************
 * C bindings
 ***************************************************/

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_table_id(tname: *const raw::c_char) -> libc::size_t {
    if tname.is_null() {
        return libc::size_t::max_value();
    };
    let table_str = ffi::CStr::from_ptr(tname).to_str().unwrap();
    match HDDlog::get_table_id(table_str) {
        Ok(relid) => relid as libc::size_t,
        Err(_) => libc::size_t::max_value(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_get_table_name(tid: libc::size_t) -> *const raw::c_char {
    match HDDlog::get_table_cname(tid) {
        Ok(name) => name.as_ptr(),
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn ddlog_run(
    workers: raw::c_uint,
    do_store: bool,
    cb: Option<
        extern "C" fn(
            arg: libc::uintptr_t,
            table: libc::size_t,
            rec: *const record::Record,
            w: libc::ssize_t,
        ),
    >,
    cb_arg: libc::uintptr_t,
    print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    debug: bool,
) -> *const HDDlog {
    if let Some(f) = cb {
        Arc::into_raw(Arc::new(HDDlog::do_run(
            workers as usize,
            do_store,
            ExternCUpdateHandler::new(f, cb_arg),
            print_err,
            if (debug) { Some(None) } else { None },
        )))
    } else {
        Arc::into_raw(Arc::new(HDDlog::do_run(
            workers as usize,
            do_store,
            NullUpdateHandler::new(),
            print_err,
            if (debug) { Some(None) } else { None },
        )))
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_run_with_debugger(
    workers: raw::c_uint,
    do_store: bool,
    cb: Option<
        extern "C" fn(
            arg: libc::uintptr_t,
            table: libc::size_t,
            rec: *const record::Record,
            w: libc::ssize_t,
        ),
    >,
    cb_arg: libc::uintptr_t,
    print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    debugger: *mut Debugger,
) -> *const HDDlog {
    if debugger.is_null() {
        return ptr::null();
    }

    if let Some(f) = cb {
        Arc::into_raw(Arc::new(HDDlog::do_run(
            workers as usize,
            do_store,
            ExternCUpdateHandler::new(f, cb_arg),
            print_err,
            Some(Some(Box::from_raw(debugger))),
        )))
    } else {
        Arc::into_raw(Arc::new(HDDlog::do_run(
            workers as usize,
            do_store,
            NullUpdateHandler::new(),
            print_err,
            Some(Some(Box::from_raw(debugger))),
        )))
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_attach_debugger(
    prog: *const HDDlog,
    debugger: *mut Debugger,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);
    let res = match Arc::get_mut(&mut prog) {
        Some(prog) => prog
            .do_attach_debugger(Box::from_raw(debugger))
            .map(|_| 0)
            .unwrap_or_else(|e| {
                prog.eprintln(&format!("ddlog_attach_debugger: error: {}", e));
                -1
            }),
        None => -1,
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_detach_debugger(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);
    let res = match Arc::get_mut(&mut prog) {
        Some(prog) => {
            prog.detach_debugger();
            0
        }
        None => -1,
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_record_commands(
    prog: *const HDDlog,
    fd: unix::io::RawFd,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let mut prog = Arc::from_raw(prog);

    let file = if fd == -1 {
        None
    } else {
        Some(fs::File::from_raw_fd(fd))
    };

    let res = match Arc::get_mut(&mut prog) {
        Some(prog) => {
            let mut old_file = file.map(Mutex::new);
            prog.record_commands(&mut old_file);
            /* Convert the old file into FD to prevent it from closing.
             * It is the caller's responsibility to close the file when
             * they are done with it. */
            old_file.map(|m| m.into_inner().unwrap().into_raw_fd());
            0
        }
        None => -1,
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_input_snapshot(
    prog: *const HDDlog,
    fd: unix::io::RawFd,
) -> raw::c_int {
    if prog.is_null() || fd < 0 {
        return -1;
    };
    let prog = Arc::from_raw(prog);
    let mut file = fs::File::from_raw_fd(fd);
    let res = prog
        .dump_input_snapshot(&mut file)
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_dump_input_snapshot: error: {}", e));
            -1
        });
    file.into_raw_fd();
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_stop(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    /* Prevents closing of the old descriptor. */
    ddlog_record_commands(prog, -1);

    let prog = Arc::from_raw(prog);
    match Arc::try_unwrap(prog) {
        Ok(HDDlog {
            prog, print_err, ..
        }) => prog
            .into_inner()
            .map(|mut p| {
                p.stop().map(|_| 0).unwrap_or_else(|e| {
                    HDDlog::print_err(print_err, &format!("ddlog_stop(): error: {}", e));
                    -1
                })
            })
            .unwrap_or_else(|e| {
                HDDlog::print_err(
                    print_err,
                    &format!("ddlog_stop(): error acquiring lock: {}", e),
                );
                -1
            }),
        Err(pref) => {
            pref.eprintln("ddlog_stop(): cannot extract value from Arc");
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_start(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.transaction_start().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_start(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes(
    prog: *const HDDlog,
    cb: Option<
        extern "C" fn(
            arg: libc::uintptr_t,
            table: libc::size_t,
            rec: *const record::Record,
            polarity: bool,
        ),
    >,
    cb_arg: libc::uintptr_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let f = cb.map(|f| {
        move |tab, rec: &record::Record, pol| f(cb_arg, tab, rec as *const record::Record, pol)
    });

    let res = prog
        .transaction_commit_dump_changes()
        .map(|changes| {
            if let Some(f) = f {
                for (table_id, table_data) in changes.as_ref().iter() {
                    for (val, weight) in table_data.iter() {
                        assert!(*weight == 1 || *weight == -1);
                        f(
                            *table_id as libc::size_t,
                            &val.clone().into_record(),
                            *weight == 1,
                        );
                    }
                }
            };
            0
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!(
                "ddlog_transaction_commit_dump_changes: error: {}",
                e
            ));
            -1
        });

    Arc::into_raw(prog);
    res
}

#[cfg(feature = "flatbuf")]
#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes_to_flatbuf(
    prog: *const HDDlog,
    buf: *mut *const u8,
    buf_size: *mut libc::size_t,
    buf_capacity: *mut libc::size_t,
    buf_offset: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null() || buf_size.is_null() || buf_capacity.is_null() || buf_offset.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog
        .transaction_commit_dump_changes()
        .map(|changes| {
            let (fbvec, fboffset) = flatbuf::updates_to_flatbuf(&changes);
            *buf = fbvec.as_ptr();
            *buf_size = fbvec.len() as libc::size_t;
            *buf_capacity = fbvec.capacity() as libc::size_t;
            *buf_offset = fboffset as libc::size_t;
            mem::forget(fbvec);
            0
        })
        .unwrap_or_else(|e| {
            prog.eprintln(&format!(
                "ddlog_transaction_commit_dump_changes_to_flatbuf: error: {}",
                e
            ));
            -1
        });

    Arc::into_raw(prog);
    res
}

#[cfg(not(feature = "flatbuf"))]
#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit_dump_changes_to_flatbuf(
    prog: *const HDDlog,
    _buf: *mut *const u8,
    _buf_size: *mut libc::size_t,
    _buf_capacity: *mut libc::size_t,
    _buf_offset: *mut libc::size_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);
    prog.eprintln("ddlog_transaction_commit_dump_changes_to_flatbuf(): error: DDlog was compiled without FlatBuffers support");
    Arc::into_raw(prog);
    -1
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_flatbuf_free(
    buf: *mut u8,
    buf_size: libc::size_t,
    buf_capacity: libc::size_t,
) {
    Vec::from_raw_parts(buf, buf_size as usize, buf_capacity as usize);
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_commit(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.transaction_commit().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_commit(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_transaction_rollback(prog: *const HDDlog) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.transaction_rollback().map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_transaction_rollback(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates(
    prog: *const HDDlog,
    upds: *const *mut record::UpdCmd,
    n: libc::size_t,
) -> raw::c_int {
    if prog.is_null() || upds.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog
        .apply_updates((0..n).map(|i| Box::from_raw(*upds.add(i))))
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_apply_updates(): error: {}", e));
            -1
        });
    Arc::into_raw(prog);
    res
}

#[cfg(feature = "flatbuf")]
#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates_from_flatbuf(
    prog: *const HDDlog,
    buf: *const u8,
    n: libc::size_t,
) -> raw::c_int {
    if prog.is_null() || buf.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog
        .apply_updates_from_flatbuf(slice::from_raw_parts(buf, n))
        .map(|_| 0)
        .unwrap_or_else(|e| {
            prog.eprintln(&format!("ddlog_apply_updates_from_flatbuf(): error: {}", e));
            -1
        });
    Arc::into_raw(prog);
    res
}

#[cfg(not(feature = "flatbuf"))]
#[no_mangle]
pub unsafe extern "C" fn ddlog_apply_updates_from_flatbuf(
    prog: *const HDDlog,
    _buf: *const u8,
    _n: libc::size_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);
    prog.eprintln(&"ddlog_apply_updates_from_flatbuf(): error: DDlog was compiled without FlatBuffers support");
    Arc::into_raw(prog);
    -1
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_clear_relation(
    prog: *const HDDlog,
    table: libc::size_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let res = prog.clear_relation(table).map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_clear_relation(): error: {}", e));
        -1
    });
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_dump_table(
    prog: *const HDDlog,
    table: libc::size_t,
    cb: Option<extern "C" fn(arg: libc::uintptr_t, rec: *const record::Record) -> bool>,
    cb_arg: libc::uintptr_t,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    let f = cb.map(|f| move |rec: &record::Record| f(cb_arg, rec));

    let res = prog.dump_table(table, f).map(|_| 0).unwrap_or_else(|e| {
        prog.eprintln(&format!("ddlog_dump_table(): error: {}", e));
        -1
    });

    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_enable_cpu_profiling(
    prog: *const HDDlog,
    enable: bool,
) -> raw::c_int {
    if prog.is_null() {
        return -1;
    };
    let prog = Arc::from_raw(prog);

    prog.enable_cpu_profiling(enable);

    Arc::into_raw(prog);
    0
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_profile(prog: *const HDDlog) -> *const raw::c_char {
    if prog.is_null() {
        return ptr::null();
    };
    let prog = Arc::from_raw(prog);

    let res = {
        let profile = prog.profile();
        ffi::CString::new(profile)
            .map(ffi::CString::into_raw)
            .unwrap_or_else(|e| {
                prog.eprintln(&format!("Failed to convert profile string to C: {}", e));
                ptr::null_mut()
            })
    };
    Arc::into_raw(prog);
    res
}

#[no_mangle]
pub unsafe extern "C" fn ddlog_string_free(s: *mut raw::c_char) {
    if s.is_null() {
        return;
    };
    ffi::CString::from_raw(s);
}
