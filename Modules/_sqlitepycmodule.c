#ifndef Py_BUILD_CORE_BUILTIN
#  define Py_BUILD_CORE_MODULE 1
#endif

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include "sqlite3.h"

static const char* PRAGMA_SQL =
    "PRAGMA page_size=16384;"
    "PRAGMA synchronous=NORMAL;"
    "PRAGMA journal_mode=WAL;"
    "PRAGMA wal_autocheckpoint=256;"
    "PRAGMA mmap_size=268435456;"  // 256 MB
    "PRAGMA user_version=1;";

static const char* SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS pyc ("
    "path TEXT PRIMARY KEY, "
    "data BLOB NOT NULL) "
    "STRICT";

static const char* GET_SQL =
    "SELECT data FROM pyc WHERE path = ?";

static const char* SET_SQL =
    "INSERT OR REPLACE INTO pyc (path, data) VALUES (?, ?)";

typedef struct _sqlitepyc_state {
    sqlite3* db;
    sqlite3_stmt* getStmt;
    sqlite3_stmt* setStmt;
} _sqlitepyc_state;

static inline _sqlitepyc_state*
get_sqlitepyc_state(PyObject* module)
{
    return (_sqlitepyc_state*) PyModule_GetState(module);
}

static PyObject*
sqlite_exception(int rc, const char* func) {
    const char* msg = sqlite3_errstr(rc);
    PyErr_Format(PyExc_RuntimeError, "sqlite-pyc: %s [%d] %s", func, rc, msg);
    return NULL;
}

static PyObject*
_sqlitepyc_init(PyObject* module, PyObject* args)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;

    // !!! Windows: The encoding used for the path argument must be UTF-8
    int rc = sqlite3_open_v2(path, &state->db, flags, NULL);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_open_v2");
    }

    rc = sqlite3_exec(state->db, PRAGMA_SQL, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_exec");
    }

    rc = sqlite3_exec(state->db, SCHEMA_SQL, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_exec");
    }

    unsigned int prepareFlags = SQLITE_PREPARE_PERSISTENT;

    rc = sqlite3_prepare_v3(state->db, GET_SQL, -1, prepareFlags, &state->getStmt, NULL);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_prepare_v3");
    }

    rc = sqlite3_prepare_v3(state->db, SET_SQL, -1, prepareFlags, &state->setStmt, NULL);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_prepare_v3");
    }

    Py_RETURN_NONE;
}

static PyObject*
_sqlitepyc_get(PyObject* module, PyObject* args)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    int rc = sqlite3_reset(state->getStmt);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_reset");
    }

    rc = sqlite3_bind_text(state->getStmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        return sqlite_exception(rc, "sqlite3_bind_text");
    }

    const void* buffer = NULL;
    int bufferSize = 0;
    PyObject* data;

    rc = sqlite3_step(state->getStmt);
    if (rc == SQLITE_ROW) {
        int type = sqlite3_column_type(state->getStmt, 0);
        if (type != SQLITE_BLOB) {
            PyErr_SetString(PyExc_RuntimeError, "unexpected column type");
            return NULL;
        }

        buffer = sqlite3_column_blob(state->getStmt, 0);
        assert(buffer != NULL);

        bufferSize = sqlite3_column_bytes(state->getStmt, 0);

        data = PyBytes_FromStringAndSize(buffer, bufferSize);
        if (data == NULL) {
            return NULL;
        }

        rc = sqlite3_step(state->getStmt);
    }
    else {
        data = Py_NewRef(Py_None);
    }

    if (rc != SQLITE_DONE) {
        Py_DECREF(data);
        return sqlite_exception(rc, "sqlite3_step");
    }

    // reset statement to release blob buffers
    rc = sqlite3_reset(state->setStmt);
    if (rc != SQLITE_OK) {
        Py_DECREF(data);
        return sqlite_exception(rc, "sqlite3_reset");
    }

    return data;
}

static PyObject*
_sqlitepyc_set(PyObject* module, PyObject* args)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    const char* path;
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "sy*", &path, &buffer))
        return NULL;

    int rc = sqlite3_reset(state->setStmt);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_reset FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_bind_text(state->setStmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_bind_text FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_bind_blob64(state->setStmt, 2, buffer.buf, buffer.len, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_bind_blob64 FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_step(state->setStmt);
    if (rc != SQLITE_DONE) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_step FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_reset(state->setStmt);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_reset FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_bind_null(state->setStmt, 1);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_bind_null FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    rc = sqlite3_bind_null(state->setStmt, 2);
    if (rc != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_bind_null FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(rc));
        return NULL;
    }

    PyBuffer_Release(&buffer);

    Py_RETURN_NONE;
}

static int
_sqlitepyc_exec(PyObject* module)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    state->db = NULL;
    state->getStmt = NULL;
    state->setStmt = NULL;

    int rc = sqlite3_initialize();
    if (rc != SQLITE_OK) {
        PyErr_SetString(PyExc_ImportError, sqlite3_errstr(rc));
        return -1;
    }

    return 0;
}

static PyMethodDef _sqlitepyc_methods[] = {
    {"init", _sqlitepyc_init, METH_VARARGS, NULL},
    {"get", _sqlitepyc_get, METH_VARARGS, NULL},
    {"set", _sqlitepyc_set, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef_Slot _sqlitepyc_slots[] = {
    {Py_mod_exec, _sqlitepyc_exec},
    {0, NULL},
};

static void
_sqlitepyc_free(void* module)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    if (state->getStmt != NULL) {
        int rc = sqlite3_finalize(state->getStmt);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_finalize FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));
        }
        state->getStmt = NULL;
    };

    if (state->setStmt != NULL) {
        int rc = sqlite3_finalize(state->setStmt);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_finalize FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));
        }
        state->setStmt = NULL;
    };

    if (state->db != NULL) {
        int rc = sqlite3_close_v2(state->db);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_close_v2 FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));
        }
        state->db = NULL;
    }

    int rc = sqlite3_shutdown();
    if (rc != SQLITE_OK) {
        fprintf(stderr, "*** sqlite3_shutdown FAILED: [%d] %s\n", rc, sqlite3_errstr(rc));
    }

    return;
}

static struct PyModuleDef _sqlitepycmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_sqlitepyc",
    .m_size = sizeof(_sqlitepyc_state),
    .m_methods = _sqlitepyc_methods,
    .m_slots = _sqlitepyc_slots,
    .m_free = _sqlitepyc_free,
};

PyMODINIT_FUNC
PyInit__sqlitepyc(void)
{
    return PyModuleDef_Init(&_sqlitepycmodule);
}
