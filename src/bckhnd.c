/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                                                      */
/*                       B A C K O U T   H A N D L E R                        */
/*                                        */
/*                              B C K H N D . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*  functions:                                                          */
/*    - backoutHandler                                          */
/*    - moveMessages                                  */
/*    - readMessage                                    */
/*    - readOldestMessage                      */
/*    - dumpMsg                              */
/*                                                                          */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include <ctl.h>
#include <msgcat/lgstd.h>
#include <mqbase.h>
#include <mqtype.h>

// ---------------------------------------------------------
// local
// ---------------------------------------------------------
#include <cmdln.h>
#include <bckhnd.h>
#include <msgmng.h>
#include <dirent.h>

#include "sighnd.h"

/******************************************************************************/
/*   G L O B A L S                                                            */
/******************************************************************************/
MQHCONN _ghCon ;                         // global connection handle   

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define MQGET_WAIT       100
#define INITIAL_MSG_SIZE 4096
#define DEFAULT_EXPIRY_TIME 3600
#define MIN_EXPIRY_TIME     600

#define MAX_OUTPUT_STR_LNG  512

#define F_KEY       "%-30.30s"    // dump format for key
#define F_STR       F_KEY"%-25.47s\n"    // dump format for general string
#define F_MQCHAR4   F_KEY"%-1.4s\n"      // dump format for MQCHAR4
#define F_MQCHAR8   F_KEY"%-8.47s\n"     // dump format for MQCHAR8
#define F_MQCHAR12  F_KEY"%-12.47s\n"     // dump format for MQCHAR12
#define F_MQCHAR28  F_KEY"%-28.47s\n"    // dump format for MQCHAR28
#define F_MQCHAR32  F_KEY"%-32.47s\n"    // dump format for MQCHAR32
#define F_MQCHAR48  F_KEY"%-48.48s\n"    // dump format for MQCHAR48
#define F_MQLONG    F_KEY"%.10d\n"       // dump format for MQLONG
#define F_MQBYTE24  F_KEY"0x %-48.48s\n"
#define F_MQBYTE32  F_KEY"0x %-64.64s\n"  // length of hex string

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
int moveMessages();

int readMessage( MQHCONN _hCon        ,  // connection handle   
                 MQHOBJ  _hGetQ       ,  // get queue handle
		 PMQMD   _pMd         ,  // message descriptor (set to default) 
                 PMQVOID *_pBuffer    ,  // message buffer
                 PMQLONG _pMaxMsgLng  ,  // maximal available message length
                 PMQLONG _pRealMsgLng);  // real message length

int readOldestMessage(MQHCONN _hCon        , // connection handle   
                      MQHOBJ  _hGetQ       , // get queue handle
                      PMQMD   _pMd         , // message descriptor 
                      PMQVOID *_pBuffer    , // message buffer
                      PMQLONG _pMaxMsgLng  , // maximal available message length
                      PMQLONG _pRealMsgLng); // real message length

void dumpMsg( const char *path,
              MQMD md         , 
              PMQVOID buffer  , 
              int  realMsgLng);

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/* get connection handle                                                      */
/******************************************************************************/
MQHCONN getConHanlder()
{
  logFuncCall() ;               
  logFuncExit( );
  return _ghCon ;
}

/******************************************************************************/
/*  back out handler                                                          */
/******************************************************************************/
int backoutHandler()
{
  logFuncCall() ;               

  int sysRc = 0 ;

  char qmgrName[MQ_Q_MGR_NAME_LENGTH+1]; // queue manager name 
                                         //
  MQHOBJ  hBoq ;                         // queue handle   
  MQOD    dBoq = {MQOD_DEFAULT};         // queue descriptor
                                         //
  MQHOBJ  hSrcq ;                        // queue handle   
  MQOD    dSrcq = {MQOD_DEFAULT};        // queue descriptor
                                         //
  MQHOBJ  hFwdq ;                        // queue handle   
  MQOD    dFwdq = {MQOD_DEFAULT};        // queue descriptor

  // -------------------------------------------------------
  // initialize message id list
  // -------------------------------------------------------
  sysRc = initMsgIdList();
  if( sysRc != 0 )
  {
    goto _door ;
  }

  // -------------------------------------------------------
  // connect to queue manager 
  // -------------------------------------------------------
  memset( qmgrName, ' ', MQ_Q_MGR_NAME_LENGTH );  
  qmgrName[MQ_Q_MGR_NAME_LENGTH] = '\0'  ;  // copy queue manager name
  memcpy( qmgrName                       ,  // from command line attributes
          getStrAttr( "qmgr" )           ,  // to vara
          strlen( getStrAttr( "qmgr" ) ));  //
                                            //
  sysRc =  mqConn( (char*) qmgrName,        // queue manager          
                           &_ghCon  );        // connection handle            
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE :     break ;            // OK
    case MQRC_Q_MGR_NAME_ERROR :            // queue manager does not exists
    {                                       //
      logger(LMQM_UNKNOWN_QMGR,qmgrName);   //
      goto _door;                           //
    }                                       //
    default : goto _door;                   // error will be logged in mqConn
  }                                         //
                                            //
  // -------------------------------------------------------
  // open back out queue
  // -------------------------------------------------------
  memset( dBoq.ObjectName, ' ', MQ_Q_NAME_LENGTH );   
  dBoq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ;  //
  memcpy( dBoq.ObjectName   ,                 //
          getStrAttr("backout")           ,   //
          strlen(getStrAttr("backout")) );    //
                                              //
  sysRc=mqOpenObject( _ghCon                  , // connection handle
                      &dBoq                 , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | // open for exclusive get
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hBoq );                // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( dSrcq.ObjectName, ' ', MQ_Q_NAME_LENGTH ); 
  dSrcq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ; //
  memcpy( dSrcq.ObjectName  ,                 //
          getStrAttr("source")          ,     //
          strlen( getStrAttr("source") ) );   //
                                              //
  sysRc=mqOpenObject( _ghCon                  , // connection handle
                      &dSrcq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hSrcq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open forward queue
  // -------------------------------------------------------
  memset( dFwdq.ObjectName, ' ', MQ_Q_NAME_LENGTH ); 
  dFwdq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ; //
  memcpy( dFwdq.ObjectName  ,                 //
          getStrAttr("forward")          ,    //
          strlen( getStrAttr("forward") ) );  //
                                              //
  sysRc=mqOpenObject( _ghCon                  , // connection handle
                      &dFwdq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hFwdq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // move messages under sync point
  // -------------------------------------------------------
  while( 1 )                                  //
  {                                           //
    sysRc = moveMessages( _ghCon ,              // move message back to source
			 hBoq  ,              //  queue or forward to goal
			 hSrcq ,              //  queue
			 hFwdq);              //
    switch( sysRc )                           //
    {                                         //
      case MQRC_NONE: continue ;              // message moved
      case MQRC_NO_MSG_AVAILABLE:             // either no message found on the 
      {                                       //  queue or matching message id
	sleep(5);                             //  failed
        continue;                             //
      }                                       //
      case MQRC_Q_FULL :                      // source or goal queue is full
      {                                       // rollback the message and wait
	sleep(60);                            // one minute and retry 1min later
	continue ;                            //
      }                                       //
      default:                                //
      {                                       //
	goto _door;                           //
      }                                       //
    }                                         //
  }

  _door:

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*   move messages                                                            */
/******************************************************************************/
int moveMessages( MQHCONN _hCon    ,     // connection handle   
                  MQHOBJ  _hGetQ   ,     // get queue handle
                  MQHOBJ  _hPutOrg ,     // put queue original handle 
                  MQHOBJ  _hPutFwd )     // get queue forward handle
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;
  int    found  ;
  int    expiry = getIntAttr("expiry") ;

  MQMD  md  = {MQMD_DEFAULT} ;    // message descriptor (set to default)
  MQPMO pmo = {MQPMO_DEFAULT};    // put message option set to default
                                  //
                                  //
  static PMQVOID buffer = NULL;   //
                                  //
  static MQLONG maxMsgLng = INITIAL_MSG_SIZE ;
         MQLONG realMsgLng ;

  if( expiry == INT_MAX || expiry == INT_MIN )
  {
    expiry = DEFAULT_EXPIRY_TIME ;
  }
  if( expiry < MIN_EXPIRY_TIME )
  {
    expiry = MIN_EXPIRY_TIME ;
  }

  // -----------------------------------------------------
  // initialization of static vara with first call of this function
  // -----------------------------------------------------
  if( !buffer )                             //
  {                                         // message buffer, allocation 
    buffer = (PMQVOID) malloc( maxMsgLng ); // necessary only on first call
    if( !buffer )                           // of this function, since static
    {                                       //
      logger( LSTD_MEM_ALLOC_ERROR );       //
      sysRc = errno ;                       //
      goto _door;                           //
    }                                       //
  }                                         //
                                            //
  // -----------------------------------------------------
  // start transaction
  // -----------------------------------------------------
  reason = mqBegin( _hCon );              // begin transaction
  switch( reason )                        //
  {                                       //
    case MQRC_NONE :                      //
    case MQRC_NO_EXTERNAL_PARTICIPANTS :  // transactions without external 
    {                                     //  resource manager
      sysRc = MQRC_NONE;                  //
      break;                              //
    }                                     //
    default :                             //
    {                                     //
      sysRc = reason ;                    //
      goto _door;                         //
    }                                     //
  }                                       //
                                          // 
  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  sysRc = readMessage( _hCon        ,       // connection handle   
                       _hGetQ       ,       // get queue handle
                       &md          ,       // message descriptor 
                       &buffer      ,       // message buffer
                       &maxMsgLng   ,       // message buffer length
                       &realMsgLng );       // real message length
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE : break;                 //
    case MQRC_NO_MSG_AVAILABLE : goto _backout;
    default        : goto _door;            //
  }                                         //
                                            //
  found = chkMsgId( md.MsgId, expiry );     //
                                            //
  pmo.Options = MQPMO_FAIL_IF_QUIESCING +   //
                MQPMO_SET_ALL_CONTEXT   +   // for keeping old time / date
                MQPMO_SYNCPOINT         ;   //
                                            //
  switch( found )                           //
  {                                         //
    // -----------------------------------------------------
    // put message back to the original queue
    // -----------------------------------------------------
    case MSG_ID_NOT_FOUND :                 //
    {                                       //
      sysRc = mqPut( _hCon       ,          // connection handle
                     _hPutOrg    ,          // original queue handle
                     &md         ,          // message descriptor
                     &pmo        ,          // Options controlling MQPUT
                     buffer      ,          // message buffer
                     realMsgLng );          // message length (buffer length)
                                            //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE:                     //
        {                                   //
	  mqCommit( _hCon );                //
          usleep(500000);                   //   0.5 sec
	  break;                            //
        }                                   //
        default: goto _backout;             //
      }                                     //
      break;                                //
    }                                       //
                                            //
    case MSG_ID_OVER_LIST_BUFFER :          //
    {                                       //
      sysRc = mqPut( _hCon       ,          // connection handle
                     _hPutOrg    ,          // original queue handle
                     &md         ,          // message descriptor
                     &pmo        ,          // Options controlling MQPUT
                     buffer      ,          // message buffer
                     realMsgLng );          // message length (buffer length)
                                            //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE:                     //
        {                                   //
	  mqCommit( _hCon );                //
	  break;                            //
        }                                   //
        default: goto _backout;             //
      }                                     //
                                            //
      sysRc = mqBegin( _hCon );             //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE: break;              //
	case MQRC_NO_EXTERNAL_PARTICIPANTS: break;
        default: goto _backout;             //
      }                                     //
                                            //
      sysRc=readOldestMessage(_hCon       , // connection handle   
                              _hGetQ      , // get queue handle
                              &md         , // message descriptor 
                              &buffer     , // message buffer
                              &maxMsgLng  , // message buffer length
                              &realMsgLng); // real message length
                                            //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE: break;              //
        case MQRC_NO_MSG_AVAILABLE:         // no message found, nothing to put
	{                                   //
          sysRc = MQRC_NONE ;               // reset return value to avoid
	}                                   //  sleep in callee function
	default:                            //
          goto _door;                       //
      }                                     //
    }                                       // no break, put to forward queue
                                            // has to be done
    // -----------------------------------------------------
    // put message to the forward queue  
    // -----------------------------------------------------
    case MSG_ID_FOUND:                      //
    {                                       //
      sysRc = mqPut( _hCon       ,          // connection handle
                     _hPutFwd    ,          // original queue handle
                     &md         ,          // message descriptor
                     &pmo        ,          // Options controlling MQPUT
                     buffer     ,           // message buffer
                     realMsgLng );          // message length (buffer length)
                                            //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE:                     //
        {                                   //
	  if( getStrAttr("dump") )          //
	  {                                 //
            dumpMsg( getStrAttr("dump"),    //
                     md          ,          //
                     buffer      ,          //
                     realMsgLng );          //
	  }                                 //
	  mqCommit( _hCon );                //
	  break;                            //
        }                                   //
        default: goto _backout;             //
      }                                     //
      break;                                //
    }                                       //
  }                                         //
                                            //
  // -------------------------------------------------------
  // function exit point error or OK
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // function exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*  read message                                                              */
/*                                                                            */
/*  description:                                                              */
/*    get a message from the queue, if available message buffer of maxMsgLng  */
/*    is not available, resize buffer                                         */
/*                                                                            */
/*   attributes:                                                              */
/*     1. _hCon       : connection handle                                     */
/*     2. _hGetQ      : get queue handle                                      */
/*     3. _pMd        : message descriptor, already set to MQMD_DEFAULT       */
/*     4. *_pBuffer   : message buffer, maxMsgLng allocated                   */
/*     5. _pMaxMsgLng : maximal available message length, can be increased    */
/*                      in readMessage by resizeMqMessageBuffer               */
/*     6. _pRealMsgLng: real message length, this information is needed by    */
/*                      later put call                                        */
/*                                                                            */
/******************************************************************************/
int readMessage( MQHCONN _hCon        ,  // connection handle   
                 MQHOBJ  _hGetQ       ,  // get queue handle
		 PMQMD   _pMd         ,  // message descriptor (set to default) 
                 PMQVOID *_pBuffer    ,  // message buffer
                 PMQLONG _pMaxMsgLng  ,  // maximal available message length
                 PMQLONG _pRealMsgLng )  // real message length
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQGMO gmo = {MQGMO_DEFAULT};    // get message option set to default

  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  memcpy( _pMd->MsgId       ,               // flash message id, 
          MQMI_NONE         ,               // 
          sizeof(MQBYTE24) );               // MQBYTE24  
  _pMd->Version = MQMD_VERSION_2;           //
                                            //
  gmo.MatchOptions = MQMO_NONE        ;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  *_pRealMsgLng = *_pMaxMsgLng;             //
                                            //
  sig4mq();                                 // set signals for MQGET  
                                            //
  reason = mqGet( _hCon       ,             // connection handle
                  _hGetQ      ,             // pointer to queue handle
                  *_pBuffer   ,             // message buffer
                  _pRealMsgLng,             // buffer length
                  _pMd        ,             // message descriptor
                  gmo         ,             // get message option
                  MQGET_WAIT );             // wait interval in milliseconds
                                            // 
  checkSigint() ;                           // check for signals, exit())
                                            //  on SIGINT exit will be called
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      sysRc = reason ;                      //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      goto _backout;                        //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      reason = mqRollback( _hCon );         // roll back 
                                            //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      reason = mqBegin( _hCon );
      switch( reason )
      {
	case MQRC_NONE: break;
	default: 
        {
          sysRc = reason ;
          goto _door;
        }
      }

      // -------------------------------------------------
      // resize buffer
      // ---------------------------------------------- // increase the buffer 
      *_pMaxMsgLng=((int)((*_pRealMsgLng)/1024)+1)*1024;// size to next full kB, 
      *_pBuffer = resizeMqMessageBuffer( *_pBuffer,     // f.e. 4500Byte to 5k
                                         _pMaxMsgLng ); // 
      if( !_pBuffer )                       //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      *_pRealMsgLng = *_pMaxMsgLng;         //
                                            //
      sig4mq();                             // set signals for MQGET
                                            //
      reason=mqGet( _hCon       ,           // connection handle
                    _hGetQ      ,           // pointer to queue handle
                    *_pBuffer   ,           // message buffer
                    _pRealMsgLng,           // buffer length
                    _pMd        ,           // message descriptor
                    gmo         ,           // get message option
                    MQGET_WAIT );           // wait interval in milliseconds
                                            //
      checkSigint();                        // check for signals, exit() will
                                            //  be called on SIGINT
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
        case MQRC_NO_MSG_AVAILABLE:         //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _backout;                    //
        }                                   //
      }                                     //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // any other error
    // ---------------------------------------------------
    default:                                //
    {                                       //
      logMQCall(ERR,"MQGET",reason);        //
      sysRc = reason;                       //
      goto _backout ;                       //
    }                                       //
  }                                         //
                                            //
  // -------------------------------------------------------
  // exit point for OK and Error
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;

}
/******************************************************************************/
/*  read message                                                              */
/*                                                                            */
/*  description:                                                              */
/*    get a message from the queue, if available message buffer of maxMsgLng  */
/*    is not available, resize buffer                                         */
/*                                                                            */
/*   attributes:                                                              */
/*     1. _hCon       : connection handle                                     */
/*     2. _hGetQ      : get queue handle                                      */
/*     3. _pMd        : message descriptor, already set to MQMD_DEFAULT       */
/*     4. *_pBuffer   : message buffer, maxMsgLng allocated                   */
/*     5. _pMaxMsgLng : maximal available message length, can be increased    */
/*                      in readMessage by resizeMqMessageBuffer               */
/*     6. _pRealMsgLng: real message length, this information is needed by    */
/*                      later put call                                        */
/*                                                                            */
/******************************************************************************/
int readOldestMessage( MQHCONN _hCon       , // connection handle   
                       MQHOBJ  _hGetQ      , // get queue handle
                       PMQMD   _pMd        , // message descriptor 
                       PMQVOID *_pBuffer   , // message buffer
                       PMQLONG _pMaxMsgLng , // maximal available message length
                       PMQLONG _pRealMsgLng) // real message length
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQGMO gmo = {MQGMO_DEFAULT};    // get message option set to default

  PMQBYTE24 pMsgId = NULL;

  pMsgId = findOldestMessage();
  if( !pMsgId ) goto _backout;

  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  memcpy( _pMd->MsgId       ,               // flash message id, 
          pMsgId            ,               // 
          sizeof(MQBYTE24) );               // MQBYTE24  
  _pMd->Version = MQMD_VERSION_2;           //
                                            //
  gmo.MatchOptions = MQMO_MATCH_MSG_ID;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  *_pRealMsgLng = *_pMaxMsgLng;             //
                                            //
  sig4mq();                                 // set signals for MQGET 
                                            //
  reason = mqGet( _hCon       ,             // connection handle
                  _hGetQ      ,             // pointer to queue handle
                  *_pBuffer   ,             // message buffer
                  _pRealMsgLng,             // buffer length
                  _pMd        ,             // message descriptor
                  gmo         ,             // get message option
                  MQGET_WAIT );             // wait interval in milliseconds
                                            // (makes 0.1 sec)
  checkSigint();                            // check for signals, exit() might
                                            //  be called on SIGINT 
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      sysRc = reason ;
      goto _backout;                        //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      goto _backout;                        //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      reason = mqRollback( _hCon );         // roll back 
                                            //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // resize buffer
      // ---------------------------------------------- // increase the buffer 
      *_pMaxMsgLng=((int)((*_pRealMsgLng)/1024)+1)*1024;// size to next full kB, 
      *_pBuffer = resizeMqMessageBuffer( *_pBuffer,     // f.e. 4500Byte to 5k
                                         _pMaxMsgLng ); // 
      if( !_pBuffer )                       //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      *_pRealMsgLng = *_pMaxMsgLng;         //
                                            //
      sig4mq();                             // set signals for MQGET 
                                            //
      reason=mqGet( _hCon       ,           // connection handle
                    _hGetQ      ,           // pointer to queue handle
                    *_pBuffer   ,           // message buffer
                    _pRealMsgLng,           // buffer length
                    _pMd        ,           // message descriptor
                    gmo         ,           // get message option
                    MQGET_WAIT );           // wait interval in milliseconds
                                            //
      checkSigint();                        // check signals, exit() might be)
                                            // called on SIGINT
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
	case MQRC_NO_MSG_AVAILABLE :        //
        {                                   //
	  sysRc = reason;                   //
          goto _backout;                    //
        }                                   //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
      }                                     //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // any other error
    // ---------------------------------------------------
    default:                                //
    {                                       //
      logMQCall(ERR,"MQGET",reason);        //
      sysRc = reason;                       //
      goto _backout ;                       //
    }                                       //
  }                                         //
                                            //
  // -------------------------------------------------------
  // exit point for OK and Error
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;

}

/******************************************************************************/
/*   dump message                                                             */
/*                                                                            */
/*   description:                                                             */
/*     dump message including message description and message body in human   */
/*     readable format to the file                                            */
/*     the file should be written on the directory path and should have       */
/*     message id in the name                                                 */
/*                                                                            */
/*   attributes:                                                              */
/*     1. path                                                                */
/*     2. message descriptor                                        */
/*     3. message buffer                                                 */
/*     4. message length                                         */
/*                                                    */
/*    return type:                                                        */
/*      void, no return code needed, if writing a file fails, no data will be */
/*      lost since real message is still on the queue                         */
/*                                                                            */
/******************************************************************************/
void dumpMsg( const char *path,
              MQMD md         , 
              PMQVOID buffer    , 
              int  realMsgLng )
{
  logFuncCall() ;               

  FILE *fp ;

  char msgId[sizeof(MQBYTE24)*2+1] ;
  msgId[sizeof(MQBYTE24)*2] = '\0';

  char corId[sizeof(MQBYTE24)*2+1] ;
  corId[sizeof(MQBYTE24)*2] = '\0';

  char acToken[sizeof(MQBYTE32)*2+1] ;
  acToken[sizeof(MQBYTE32)*2] = '\0';

  char grpId[sizeof(MQBYTE24)*2+1] ;
  grpId[sizeof(MQBYTE24)*2] = '\0';

  char fileName[MAXNAMLEN];
  char outStr[MAX_OUTPUT_STR_LNG];

  int outStrLng ;

  int i;

  for(i=0;i<24;i++) { sprintf(&msgId[i*2],"%.2x",(int)md.MsgId[i]); }
  for(i=0;i<24;i++) { sprintf(&corId[i*2],"%.2x",(int)md.CorrelId[i]); }
  for(i=0;i<24;i++) { sprintf(&grpId[i*2],"%.2x",(int)md.GroupId[i]); }
  for(i=0;i<32;i++) { sprintf(&acToken[i*2],"%.2x",(int)md.AccountingToken[i]);}

  snprintf( fileName, MAXNAMLEN, "%s/%s.%s.browse", path,
                                 getStrAttr("source")   ,
                                 msgId                  );

  fp = fopen(fileName,"w");
  if( !fp )
  {
    logger(LSTD_OPEN_FILE_FAILED,fileName);
    logger( LSTD_ERRNO_ERR, errno, strerror(errno) );
    goto _door;
  }

  fprintf(fp,F_MQCHAR4,"Structure identifier",md.StrucId );           
  fprintf(fp,F_STR,"Structure version"  ,(char*)mqmdVer2str(md.Version));
  fprintf(fp,F_STR,"Report message options",(char*)mqReportOption2str(md.Report));
  fprintf(fp,F_STR,"Message type"       ,(char*)mqMsgType2str(md.MsgType));  
  fprintf(fp,F_MQLONG,"Message lifetime",md.Expiry );
  fprintf(fp,F_STR,"Feedback code"      ,(char*)mqFeedback2str( md.Feedback));
  fprintf(fp,F_STR,"Numeric encoding"   ,(char*) mqEncondig2str(md.Encoding));
  fprintf(fp,F_STR,"Message data CCSID" ,(char*)mqCCSID2str(md.CodedCharSetId));
  fprintf(fp,F_MQCHAR8,"Message data Format name",md.Format);            
  fprintf(fp,F_STR,"Message priority",(char*)mqPriority2str(md.Priority));
  fprintf(fp,F_STR,"Message persistence",
                   (char*)mqPersistence2str(md.Persistence));
  fprintf(fp,F_MQBYTE24,"Message identifier",msgId);
  fprintf(fp,F_MQBYTE24,"Correlation identifier",corId);
  fprintf(fp,F_MQLONG,"Backout counter",md.BackoutCount);
  fprintf(fp,F_MQCHAR48,"Name of reply queue",md.ReplyToQ);
  fprintf(fp,F_MQCHAR48, "Name of reply qmgr",md.ReplyToQMgr);
  fprintf(fp,F_MQCHAR12,"User identifier",md.UserIdentifier);
  fprintf(fp,F_MQBYTE32,"Accounting token",acToken);
  fprintf(fp,F_MQCHAR32,"Appl data relating identity",md.ApplIdentityData);
  fprintf(fp,F_STR,"Appl Put Type",(char*)mqPutApplType2str(md.PutApplType));
  fprintf(fp,F_MQCHAR28,"Putting appl name",md.PutApplName);       
  fprintf(fp,F_MQCHAR8,"Put Date",md.PutDate  );
  fprintf(fp,F_MQCHAR8,"Put time",md.PutTime   );
  fprintf(fp,F_MQCHAR4,"Appl data relating to origin",md.ApplOriginData);

  // -------------------------------------------------------
  // msg dscr version 2 or higher
  // -------------------------------------------------------
  if( md.Version < MQMD_VERSION_2 ) goto _message ;
  fprintf(fp,F_MQBYTE24,"Group identifier",grpId);
  fprintf(fp,F_MQLONG,"Logical SeqNr in group",md.MsgSeqNumber);
  fprintf(fp,F_MQLONG,"Physical offset from logic start", md.Offset);
  fprintf(fp,F_STR, "Message flags",(char*)mqMsgFlag2str(md.MsgFlags));
  fprintf(fp,F_MQLONG,"Length of original message",md.OriginalLength);

  _message:

  outStrLng = MAX_OUTPUT_STR_LNG ;
  if( realMsgLng < MAX_OUTPUT_STR_LNG ) outStrLng = realMsgLng ;
  snprintf( outStr, outStrLng+1,"%s", (char*)buffer);
  outStr[outStrLng] = '\0';

  fprintf(fp,"-------- message body --------\n");
  fprintf(fp,"%s\n",outStr);
  if( realMsgLng > 512 ) 
    fprintf(fp," %d bytes truncated",(realMsgLng-512));
  _door:

  if(fp) { fclose(fp); } 
  logFuncExit( );
}